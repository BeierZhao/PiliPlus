import 'dart:async';
import 'dart:io';
import 'dart:math';

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
  });

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

  Future<int> ensureContentLength([BtrCancelToken? cancelToken]) async {
    if (_totalLength != null && _totalLength! > 0) {
      return _totalLength!;
    }

    final token = cancelToken ?? BtrCancelToken();
    final meta = await downloader.probeMetadata(
      candidateUrls: candidateUrls,
      cancelToken: token,
    );
    _totalLength = meta.totalLength;
    return _totalLength!;
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
          isAudio ? 'audio/mp4' : 'video/mp4',
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
        isAudio ? 'audio/mp4' : 'video/mp4',
      );
      if (isPartial) {
        request.response.headers.set(
          HttpHeaders.contentRangeHeader,
          'bytes $start-$end/$total',
        );
      }

      // Stream data in sliding window chunks with backpressure
      await _streamRange(
        start: start,
        end: end,
        response: request.response,
        cancelToken: cancelToken,
      );
    } catch (e) {
      if (!cancelToken.isCancelled) {
        try {
          request.response.statusCode = HttpStatus.internalServerError;
          await request.response.close();
        } catch (_) {}
      }
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

    final int concurrency = config.effectiveConcurrency;
    // Chunk size: between 128KB and 512KB for smooth streaming
    final int chunkSize = config.minChunkBytes.clamp(128 * 1024, 512 * 1024);
    final chunks = BtrRangeUtils.splitRange(start, end, concurrency * 4, minChunkBytes: chunkSize);

    // Sliding window buffer: max 30MB ahead
    final maxWindowChunks = max(concurrency, (config.maxBufferBytes / chunkSize).floor());
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
        );

        activeDownloads[chunkIndex] = future;
      }
    }

    // Initial fill
    fillWindow();

    for (var i = 0; i < chunks.length; i++) {
      if (cancelToken.isCancelled) break;

      fillWindow();

      final future = activeDownloads.remove(i);
      if (future == null) break;

      final chunkResult = await future;
      if (cancelToken.isCancelled) break;

      response.add(chunkResult.bytes);
      await response.flush(); // Backpressure: waits until socket buffer is drained
    }

    if (!cancelToken.isCancelled) {
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
