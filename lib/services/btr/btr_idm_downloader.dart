import 'dart:async';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'btr_cdn_resolver.dart';
import 'btr_config.dart';
import 'btr_range.dart';
import 'btr_stats.dart';

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

    Object? lastError;
    final triedUrls = <String>{};

    for (var attempt = 0; attempt < candidateUrls.length; attempt++) {
      if (cancelToken.isCancelled) {
        throw const SocketException('Task cancelled');
      }

      final rangeAvailable = resolver.rangeCandidates(candidateUrls);
      final untriedRange = rangeAvailable.where((u) => !triedUrls.contains(u)).toList();
      final pool = untriedRange.isNotEmpty
          ? untriedRange
          : candidateUrls.where((u) => !triedUrls.contains(u)).toList();
      if (pool.isEmpty) break;

      final primaryUrl = pool[chunk.index % pool.length];
      triedUrls.add(primaryUrl);

      final rescuePool = pool.where((u) => u != primaryUrl).toList();
      final rescueUrl = rescuePool.isNotEmpty ? rescuePool.first : null;

      final chunkCompleter = Completer<BtrChunkResult>();
      final primaryCancel = BtrCancelToken();
      final rescueCancel = BtrCancelToken();
      Timer? hedgeTimer;

      void onMasterCancel() {
        primaryCancel.cancel();
        rescueCancel.cancel();
        hedgeTimer?.cancel();
        if (!chunkCompleter.isCompleted) {
          chunkCompleter.completeError(const SocketException('Task cancelled'));
        }
      }

      cancelToken.addListener(onMasterCancel);

      var primaryDone = false;
      var rescueDone = false;
      var primaryFailed = false;
      var rescueFailed = false;
      Object? primaryErr;
      Object? rescueErr;

      void checkBothFailed() {
        final hasRescue = rescueUrl != null;
        if (primaryFailed && (!hasRescue || rescueFailed)) {
          if (!chunkCompleter.isCompleted) {
            chunkCompleter.completeError(
              primaryErr ?? rescueErr ?? const SocketException('Both primary and rescue failed'),
            );
          }
        }
      }

      void startRescue() {
        if (rescueDone || rescueUrl == null || chunkCompleter.isCompleted || cancelToken.isCancelled) return;
        rescueDone = true;
        unawaited(() async {
          try {
            final res = await _executeHttpRange(
              chunk: chunk,
              url: rescueUrl,
              cancelToken: rescueCancel,
            );
            if (!chunkCompleter.isCompleted) {
              primaryCancel.cancel();
              BtrStats.instance.onHedgeRescue();
              resolver.recordSuccess(rescueUrl, res.bytes.length / 0.5);
              chunkCompleter.complete(res);
            }
          } catch (e) {
            rescueFailed = true;
            rescueErr = e;
            resolver.recordFailure(rescueUrl, e, 0);
            checkBothFailed();
          }
        }());
      }

      // Start primary attempt
      unawaited(() async {
        try {
          final res = await _executeHttpRange(
            chunk: chunk,
            url: primaryUrl,
            cancelToken: primaryCancel,
          );
          primaryDone = true;
          hedgeTimer?.cancel();
          rescueCancel.cancel();
          if (!chunkCompleter.isCompleted) {
            resolver.recordSuccess(primaryUrl, res.bytes.length / 0.5);
            chunkCompleter.complete(res);
          }
        } catch (e) {
          primaryFailed = true;
          primaryErr = e;
          resolver.recordFailure(primaryUrl, e, 0);
          hedgeTimer?.cancel();
          if (rescueUrl != null && !rescueDone) {
            // Primary failed: trigger rescue immediately without waiting for delay
            startRescue();
          } else {
            checkBothFailed();
          }
        }
      }());

      if (rescueUrl != null) {
        hedgeTimer = Timer(Duration(milliseconds: config.hedgeDelayMs), () {
          if (!primaryDone && !chunkCompleter.isCompleted) {
            startRescue();
          }
        });
      }

      try {
        final result = await chunkCompleter.future;
        hedgeTimer?.cancel();
        primaryCancel.cancel();
        rescueCancel.cancel();
        cancelToken.removeListener(onMasterCancel);
        return result;
      } catch (e) {
        lastError = e;
        hedgeTimer?.cancel();
        primaryCancel.cancel();
        rescueCancel.cancel();
        cancelToken.removeListener(onMasterCancel);
        if (cancelToken.isCancelled) rethrow;
      }
    }

    throw lastError ?? SocketException('All candidate URLs failed for chunk ${chunk.index}');
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
      request.headers.set(
        'User-Agent',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      );
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

      final int? totalLength;
      if (statusCode == HttpStatus.partialContent) {
        final contentRangeHeader = response.headers.value(HttpHeaders.contentRangeHeader);
        final parsedRange = BtrRangeUtils.parseContentRange(contentRangeHeader);
        totalLength = parsedRange?.total;
      } else {
        totalLength = response.contentLength > 0 ? response.contentLength : null;
      }

      await for (final data in response) {
        resetStallTimer();
        bytesBuilder.add(data);
        received += data.length;
        onProgress?.call(received);
        BtrStats.instance.onBytesReceived(data.length);
        if (statusCode == HttpStatus.ok && received >= chunk.length) {
          try {
            request.abort();
          } catch (_) {}
          break;
        }
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
    final probeChunk = const BtrChunk(index: 0, start: 0, end: 1);
    final completer = Completer<({int totalLength, String workingUrl})>();
    final probeCancel = BtrCancelToken();
    cancelToken.addListener(probeCancel.cancel);

    final pool = candidateUrls.take(4).toList();
    var errors = 0;

    void checkError() {
      errors++;
      if (errors >= pool.length && !completer.isCompleted) {
        completer.completeError(const SocketException('All probe candidates failed'));
      }
    }

    for (final url in pool) {
      if (completer.isCompleted || probeCancel.isCancelled) break;
      unawaited(() async {
        try {
          final result = await _executeHttpRange(
            chunk: probeChunk,
            url: url,
            cancelToken: probeCancel,
          );
          if (result.totalLength != null && result.totalLength! > 0) {
            if (!completer.isCompleted) {
              probeCancel.cancel(); // Cancel other racing probes immediately to free semaphore slots
              completer.complete((totalLength: result.totalLength!, workingUrl: url));
            }
          } else {
            checkError();
          }
        } catch (e) {
          checkError();
        }
      }());
      await Future.delayed(const Duration(milliseconds: 100));
    }

    try {
      return await completer.future.timeout(
        Duration(milliseconds: max(config.attemptTimeoutMs, 10000)),
        onTimeout: () {
          probeCancel.cancel();
          throw const SocketException('Metadata probe timed out');
        },
      );
    } finally {
      probeCancel.cancel();
      cancelToken.removeListener(probeCancel.cancel);
    }
  }

  void dispose() {
    _httpClient.close(force: true);
  }
}
