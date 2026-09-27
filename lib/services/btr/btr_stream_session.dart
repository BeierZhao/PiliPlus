import 'dart:async';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'btr_cdn_resolver.dart';
import 'btr_config.dart';
import 'btr_idm_downloader.dart';
import 'btr_range.dart';

class BtrStreamSession {
  final String sessionId;
  final String originalUrl;
  final List<String> backupUrls;
  final bool isAudio;
  final BtrConfig config;
  final BtrCdnResolver resolver;
  final BtrIdmDownloader downloader;
  final String? preferredHost;

  int? _totalLength;
  List<String>? _resolvedCandidateUrls;
  final List<BtrCancelToken> _activeTokens = [];
  bool _isClosed = false;

  BtrStreamSession({
    required this.sessionId,
    required this.originalUrl,
    required this.backupUrls,
    required this.isAudio,
    required this.config,
    required this.resolver,
    required this.downloader,
    this.preferredHost,
  }) {
    // Eagerly pre-probe metadata in background right upon session creation
    unawaited(ensureContentLength().catchError((_) => 0));
  }

  bool get isClosed => _isClosed;

  List<String> get candidateUrls {
    return _resolvedCandidateUrls ??= resolver.resolveCandidateUrls(
      primaryUrl: originalUrl,
      backupUrls: backupUrls,
      mode: config.mode,
      customHosts: config.customHosts,
      preferredHost: preferredHost,
    );
  }

  Future<int>? _contentLengthFuture;

  String get _contentType {
    if (isAudio) return 'audio/mp4';
    if (originalUrl.contains('.flv')) return 'video/x-flv';
    return 'video/mp4';
  }

  Future<int> ensureContentLength([BtrCancelToken? cancelToken]) {
    if (_totalLength != null && _totalLength! > 0) {
      return Future.value(_totalLength!);
    }
    return _contentLengthFuture ??= () async {
      final token = cancelToken ?? BtrCancelToken();
      try {
        final meta = await downloader.probeMetadata(
          candidateUrls: candidateUrls,
          cancelToken: token,
        );
        _totalLength = meta.totalLength;

        // Promote the verified working URL to the front of candidate list
        if (_resolvedCandidateUrls != null) {
          _resolvedCandidateUrls!.remove(meta.workingUrl);
          _resolvedCandidateUrls!.insert(0, meta.workingUrl);
        }
        return _totalLength!;
      } catch (e) {
        _contentLengthFuture = null;
        rethrow;
      }
    }();
  }

  Future<void> handleHttpRequest(HttpRequest request) async {
    if (_isClosed) {
      request.response.statusCode = HttpStatus.gone;
      await request.response.close();
      return;
    }

    final cancelToken = BtrCancelToken();
    _activeTokens.add(cancelToken);

    // Watch for client disconnect (Seek or player stopped)
    unawaited(request.response.done.then((_) {
      cancelToken.cancel();
      _activeTokens.remove(cancelToken);
    }).catchError((_) {
      cancelToken.cancel();
      _activeTokens.remove(cancelToken);
    }));

    try {
      final total = await ensureContentLength(cancelToken);

      final rangeHeader = request.headers.value(HttpHeaders.rangeHeader);
      final range = BtrRangeUtils.parseRangeHeader(rangeHeader);

      final int start;
      final int end;

      if (range != null) {
        start = range.start.clamp(0, total - 1);
        end = (range.end != null ? range.end! : total - 1).clamp(start, total - 1);
      } else {
        start = 0;
        end = total - 1;
      }

      final contentLength = end - start + 1;

      // Handle HEAD request
      if (request.method.toUpperCase() == 'HEAD') {
        request.response.statusCode = range != null ? HttpStatus.partialContent : HttpStatus.ok;
        request.response.headers.set(HttpHeaders.acceptRangesHeader, 'bytes');
        request.response.headers.set(HttpHeaders.contentLengthHeader, contentLength.toString());
        request.response.headers.set(
          HttpHeaders.contentTypeHeader,
          _contentType,
        );
        if (range != null) {
          request.response.headers.set(
            HttpHeaders.contentRangeHeader,
            'bytes $start-$end/$total',
          );
        }
        await request.response.close();
        return;
      }

      // Handle GET request
      final isPartial = range != null;
      request.response.statusCode = isPartial ? HttpStatus.partialContent : HttpStatus.ok;
      request.response.headers.set(HttpHeaders.acceptRangesHeader, 'bytes');
      request.response.headers.set(HttpHeaders.contentLengthHeader, contentLength.toString());
      request.response.headers.set(
        HttpHeaders.contentTypeHeader,
        _contentType,
      );
      if (isPartial) {
        request.response.headers.set(
          HttpHeaders.contentRangeHeader,
          'bytes $start-$end/$total',
        );
      }

      // CRITICAL: Flush response headers immediately to client socket
      // so MPV receives HTTP/1.1 206 Partial Content instantly and never times out!
      await request.response.flush();

      // Stream data in sliding window chunks with backpressure
      await _streamRange(
        start: start,
        end: end,
        response: request.response,
        cancelToken: cancelToken,
      );
    } catch (e, st) {
      if (!cancelToken.isCancelled && e is! SocketException && e is! HttpException) {
        print('BTR StreamSession Error: $e\n$st');
      }
      try {
        if (!cancelToken.isCancelled) {
          request.response.statusCode = HttpStatus.internalServerError;
        }
      } catch (_) {}
      try {
        await request.response.close();
      } catch (_) {}
    } finally {
      cancelToken.cancel();
      _activeTokens.remove(cancelToken);
    }
  }

  /// Stream chunks sequentially to response while downloading ahead in a sliding window
  Future<void> _streamRange({
    required int start,
    required int end,
    required HttpResponse response,
    required BtrCancelToken cancelToken,
  }) async {
    final rangeLen = end - start + 1;
    if (rangeLen <= 0) {
      await response.close();
      return;
    }

    final int concurrency = isAudio ? min(3, config.effectiveConcurrency) : config.effectiveConcurrency;
    // Chunk size: 256KB for video, 128KB for audio. Initial chunk is 128KB for instant first-byte delivery
    final int chunkSize = isAudio ? 128 * 1024 : 256 * 1024;
    final chunks = BtrRangeUtils.splitIntoStreamingChunks(
      start,
      end,
      chunkSize: chunkSize,
      firstChunkSize: 128 * 1024,
    );

    print('[BTR Stream] Session $sessionId streaming range: $start-$end ($rangeLen bytes, ${chunks.length} chunks, ${isAudio ? "audio" : "video"})');

    // Sliding window buffer: max concurrency * 2 ahead (prevent exhausting memory or network)
    final maxWindowChunks = isAudio ? 4 : max(concurrency, min(concurrency * 2, 16));
    final activeDownloads = <int, Future<BtrChunkResult>>{};

    int nextToDownload = 0;

    void fillWindow() {
      while (!cancelToken.isCancelled &&
          nextToDownload < chunks.length &&
          activeDownloads.length < maxWindowChunks) {
        final chunkIndex = nextToDownload++;
        final chunk = chunks[chunkIndex];

        final future = downloader.downloadChunk(
          chunk: chunk,
          candidateUrls: candidateUrls,
          cancelToken: cancelToken,
          isAudio: isAudio,
          priority: chunkIndex == 0,
        );

        // Prevent unhandled async exceptions if session is cancelled while futures are pending
        unawaited(future.catchError((_) => BtrChunkResult(bytes: Uint8List(0), url: '')));

        activeDownloads[chunkIndex] = future;
      }
    }

    // Initial fill
    fillWindow();

    var totalBytesSent = 0;
    for (var i = 0; i < chunks.length; i++) {
      if (cancelToken.isCancelled) break;

      fillWindow();

      final future = activeDownloads.remove(i);
      if (future == null) break;

      final chunkResult = await future;
      if (cancelToken.isCancelled) break;

      // CRITICAL: Strict chunk length check to prevent bitstream corruption!
      if (chunkResult.bytes.length != chunks[i].length) {
        throw SocketException(
          '[BTR] Chunk $i size mismatch: expected ${chunks[i].length}, got ${chunkResult.bytes.length}',
        );
      }

      response.add(chunkResult.bytes);
      totalBytesSent += chunkResult.bytes.length;
      await response.flush(); // Backpressure: waits until socket buffer is drained
    }

    if (!cancelToken.isCancelled) {
      print('[BTR Stream] Session $sessionId completed: sent $totalBytesSent/$rangeLen bytes');
      await response.close();
    }
  }

  void cancelAll() {
    _isClosed = true;
    for (final token in _activeTokens) {
      token.cancel();
    }
    _activeTokens.clear();
  }
}
