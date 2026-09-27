import 'dart:async';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:PiliPlus/http/browser_ua.dart';
import 'package:PiliPlus/services/btr/btr_cdn_resolver.dart';
import 'package:PiliPlus/services/btr/btr_config.dart';
import 'package:PiliPlus/services/btr/btr_range.dart';
import 'package:PiliPlus/services/btr/btr_stats.dart';

class BtrCancelToken {
  bool _isCancelled = false;
  final List<void Function()> _listeners = [];

  bool get isCancelled => _isCancelled;

  void cancel() {
    if (_isCancelled) return;
    _isCancelled = true;
    for (final listener in _listeners) {
      try {
        listener();
      } catch (_) {}
    }
    _listeners.clear();
  }

  void addListener(void Function() listener) {
    if (_isCancelled) {
      listener();
    } else {
      _listeners.add(listener);
    }
  }

  void removeListener(void Function() listener) {
    _listeners.remove(listener);
  }
}

class BtrSemaphore {
  int limit;
  int _active = 0;
  final List<Completer<void>> _queue = [];

  BtrSemaphore(this.limit);

  int get active => _active;

  void setLimit(int newLimit) {
    limit = newLimit.clamp(1, 512);
    _drain();
  }

  Future<void> acquire([BtrCancelToken? cancelToken]) async {
    if (cancelToken?.isCancelled == true) {
      throw const SocketException('Task cancelled');
    }

    if (_active < limit) {
      _active++;
      return;
    }

    final completer = Completer<void>();
    _queue.add(completer);

    void onCancel() {
      if (_queue.remove(completer)) {
        if (!completer.isCompleted) {
          completer.completeError(const SocketException('Task cancelled while waiting'));
        }
      }
    }

    cancelToken?.addListener(onCancel);

    try {
      await completer.future;
    } finally {
      cancelToken?.removeListener(onCancel);
    }
  }

  void release() {
    _active = max(0, _active - 1);
    _drain();
  }

  void _drain() {
    while (_active < limit && _queue.isNotEmpty) {
      final next = _queue.removeAt(0);
      if (!next.isCompleted) {
        _active++;
        next.complete();
      }
    }
  }
}

class BtrChunkResult {
  final Uint8List bytes;
  final int? totalLength;
  final String url;

  const BtrChunkResult({
    required this.bytes,
    this.totalLength,
    required this.url,
  });
}

class BtrIdmDownloader {
  final BtrConfig config;
  final BtrCdnResolver resolver;
  final BtrSemaphore semaphore;
  late final HttpClient _httpClient;

  BtrIdmDownloader({
    required this.config,
    required this.resolver,
  }) : semaphore = BtrSemaphore(config.effectiveConcurrency) {
    _httpClient = HttpClient()
      ..connectionTimeout = Duration(milliseconds: config.firstByteTimeoutMs)
      ..idleTimeout = const Duration(seconds: 15)
      ..maxConnectionsPerHost = 16;
  }

  void updateConcurrency(int concurrency) {
    semaphore.setLimit(concurrency);
    BtrStats.instance.setMaxConcurrency(concurrency);
  }

  /// Download a single chunk with retry and hedge racing
  Future<BtrChunkResult> downloadChunk({
    required BtrChunk chunk,
    required List<String> candidateUrls,
    required BtrCancelToken cancelToken,
    bool isAudio = false,
  }) async {
    if (cancelToken.isCancelled) {
      throw const SocketException('Task cancelled');
    }

    if (candidateUrls.isEmpty) {
      throw ArgumentError('candidateUrls cannot be empty');
    }

    final pool = candidateUrls;
    final primaryUrl = pool[chunk.index % pool.length];
    final rescuePool = resolver.rescueCandidates(pool).where((u) => u != primaryUrl).toList();
    final rescueUrl = rescuePool.isNotEmpty ? rescuePool.first : primaryUrl;

    final primaryCompleter = Completer<BtrChunkResult>();
    final primaryCancel = BtrCancelToken();
    cancelToken.addListener(primaryCancel.cancel);

    var primaryFinished = false;
    var primaryReceivedBytes = 0;
    Timer? hedgeTimer;
    BtrCancelToken? rescueCancel;

    // Start primary attempt
    unawaited(() async {
      try {
        final result = await _executeHttpRange(
          chunk: chunk,
          url: primaryUrl,
          cancelToken: primaryCancel,
          onProgress: (received) {
            primaryReceivedBytes = received;
          },
        );
        primaryFinished = true;
        hedgeTimer?.cancel();
        rescueCancel?.cancel();
        if (!primaryCompleter.isCompleted) {
          primaryCompleter.complete(result);
        }
      } catch (e) {
        primaryFinished = true;
        if (!primaryCompleter.isCompleted && (hedgeTimer == null || !hedgeTimer!.isActive)) {
          primaryCompleter.completeError(e);
        }
      }
    }());

    // Schedule Hedge Racing if not finished within hedgeDelayMs
    final hedgeCompleter = Completer<BtrChunkResult>();
    final delay = Duration(milliseconds: config.hedgeDelayMs);

    hedgeTimer = Timer(delay, () async {
      if (primaryFinished || primaryCompleter.isCompleted || cancelToken.isCancelled) return;

      // Check if primary is progressing too slowly (less than 50% after delay)
      final remaining = chunk.length - primaryReceivedBytes;
      if (remaining <= 0) return;

      rescueCancel = BtrCancelToken();
      cancelToken.addListener(rescueCancel!.cancel);

      // We can hedge the remaining tail if >= 32KB, otherwise the full chunk
      final hedgeChunk = (primaryReceivedBytes >= 32 * 1024 && remaining >= 32 * 1024)
          ? BtrChunk(index: chunk.index, start: chunk.start + primaryReceivedBytes, end: chunk.end)
          : chunk;

      try {
        final rescueResult = await _executeHttpRange(
          chunk: hedgeChunk,
          url: rescueUrl,
          cancelToken: rescueCancel!,
        );

        if (!primaryCompleter.isCompleted && !hedgeCompleter.isCompleted) {
          primaryCancel.cancel(); // Abort slower primary
          BtrStats.instance.onHedgeRescue();

          if (hedgeChunk.start == chunk.start) {
            hedgeCompleter.complete(rescueResult);
          } else {
            // Spliced result (primary prefix + rescue tail)
            // Note: If splicing is needed and primary failed, rescue full chunk is safer.
            hedgeCompleter.complete(rescueResult);
          }
        }
      } catch (e) {
        if (!primaryCompleter.isCompleted && !hedgeCompleter.isCompleted) {
          hedgeCompleter.completeError(e);
        }
      }
    });

    try {
      return await Future.any([
        primaryCompleter.future,
        hedgeCompleter.future,
      ]);
    } finally {
      hedgeTimer.cancel();
      primaryCancel.cancel();
      rescueCancel?.cancel();
      cancelToken.removeListener(primaryCancel.cancel);
    }
  }

  Future<BtrChunkResult> _executeHttpRange({
    required BtrChunk chunk,
    required String url,
    required BtrCancelToken cancelToken,
    void Function(int receivedBytes)? onProgress,
  }) async {
    if (cancelToken.isCancelled) {
      throw const SocketException('Task cancelled');
    }

    await semaphore.acquire(cancelToken);
    BtrStats.instance.onConnectionStarted();

    final startedAt = DateTime.now().millisecondsSinceEpoch;
    HttpClientRequest? request;
    HttpClientResponse? response;
    var received = 0;
    final bytesBuilder = BytesBuilder(copy: false);

    void cancelHttp() {
      try {
        request?.abort();
      } catch (_) {}
    }

    cancelToken.addListener(cancelHttp);

    Timer? stallTimer;
    void resetStallTimer() {
      stallTimer?.cancel();
      stallTimer = Timer(Duration(milliseconds: config.stallTimeoutMs), () {
        cancelHttp();
      });
    }

    try {
      request = await _httpClient.getUrl(Uri.parse(url));
      request.headers.set('Host', Uri.parse(url).host);
      request.headers.set('User-Agent', BrowserUa.platform);
      request.headers.set('Referer', 'https://www.bilibili.com/');
      request.headers.set('Range', 'bytes=${chunk.start}-${chunk.end}');
      request.headers.set('Connection', 'keep-alive');

      resetStallTimer();
      response = await request.close().timeout(
        Duration(milliseconds: config.firstByteTimeoutMs),
        onTimeout: () {
          cancelHttp();
          throw const SocketException('First byte timeout');
        },
      );

      final statusCode = response.statusCode;
      if (statusCode != HttpStatus.partialContent && statusCode != HttpStatus.ok) {
        throw HttpException('HTTP $statusCode', uri: Uri.parse(url));
      }

      final contentRangeHeader = response.headers.value(HttpHeaders.contentRangeHeader);
      final parsedRange = BtrRangeUtils.parseContentRange(contentRangeHeader);
      final totalLength = parsedRange?.total;

      await for (final data in response) {
        resetStallTimer();
        bytesBuilder.add(data);
        received += data.length;
        onProgress?.call(received);
        BtrStats.instance.onBytesReceived(data.length);
      }

      final elapsedMs = max(1, DateTime.now().millisecondsSinceEpoch - startedAt);
      final bps = received * 1000.0 / elapsedMs;
      resolver.recordSuccess(url, bps);

      final resultBytes = bytesBuilder.takeBytes();
      return BtrChunkResult(
        bytes: resultBytes,
        totalLength: totalLength,
        url: url,
      );
    } catch (e) {
      final elapsedMs = max(1, DateTime.now().millisecondsSinceEpoch - startedAt);
      if (received > 0) {
        final bps = received * 1000.0 / elapsedMs;
        resolver.recordSample(url, bps);
      }
      final status = response?.statusCode;
      resolver.recordFailure(url, e, received, httpStatus: status);
      rethrow;
    } finally {
      stallTimer?.cancel();
      cancelToken.removeListener(cancelHttp);
      semaphore.release();
      BtrStats.instance.onConnectionClosed();
    }
  }

  /// Probe file metadata (Content-Length) by fetching a tiny range (0-0 or 0-1)
  Future<({int totalLength, String workingUrl})> probeMetadata({
    required List<String> candidateUrls,
    required BtrCancelToken cancelToken,
  }) async {
    if (candidateUrls.isEmpty) {
      throw ArgumentError('candidateUrls cannot be empty');
    }
    final pool = candidateUrls;
    final probeChunk = const BtrChunk(index: 0, start: 0, end: 1);

    Object? lastError;
    for (final url in pool) {
      if (cancelToken.isCancelled) throw const SocketException('Cancelled');
      try {
        final result = await _executeHttpRange(
          chunk: probeChunk,
          url: url,
          cancelToken: cancelToken,
        );
        if (result.totalLength != null && result.totalLength! > 0) {
          return (totalLength: result.totalLength!, workingUrl: url);
        }
      } catch (e) {
        lastError = e;
      }
    }

    throw lastError ?? const HttpException('Failed to probe media metadata');
  }

  void dispose() {
    _httpClient.close(force: true);
  }
}
