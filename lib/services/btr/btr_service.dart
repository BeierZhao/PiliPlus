import 'dart:async';
import 'btr_cdn_resolver.dart';
import 'btr_config.dart';
import 'btr_idm_downloader.dart';
import 'btr_proxy_server.dart';
import 'btr_range.dart';
import 'btr_stats.dart';
import 'btr_stream_session.dart';
import '../../utils/storage_pref.dart';

class BtrService {
  static final BtrService instance = BtrService._();
  BtrService._() {
    _init();
  }

  late BtrConfig _config;
  late final BtrCdnResolver _resolver;
  late final BtrIdmDownloader _downloader;
  late final BtrProxyServer _proxyServer;

  int _sessionCounter = 0;

  bool get isEnabled => Pref.enableBtr;

  BtrConfig get config => _config;

  BtrProxyServer get proxyServer => _proxyServer;

  BtrCdnResolver get resolver => _resolver;

  void _init() {
    _config = BtrConfig(
      enabled: Pref.enableBtr,
      mode: BtrMode.fromString(Pref.btrMode),
      concurrency: Pref.btrConcurrency,
      customHosts: Pref.btrCustomHosts,
    );
    _resolver = BtrCdnResolver();
    _downloader = BtrIdmDownloader(config: _config, resolver: _resolver);
    _proxyServer = BtrProxyServer();

    if (isEnabled) {
      unawaited(_proxyServer.start());
    }
  }

  void updateConfig({
    bool? enabled,
    BtrMode? mode,
    int? concurrency,
    List<String>? customHosts,
  }) {
    _config = _config.copyWith(
      enabled: enabled,
      mode: mode,
      concurrency: concurrency,
      customHosts: customHosts,
    );

    _downloader.updateConcurrency(_config.effectiveConcurrency);

    if (_config.enabled && !_proxyServer.isRunning) {
      unawaited(_proxyServer.start());
    } else if (!_config.enabled && _proxyServer.isRunning) {
      clearActiveSessions();
    }
  }

  /// Wrap the original Bilibili media URL with local BTR proxy URL
  String wrapUrl({
    required String originalUrl,
    required Iterable<String> backupUrls,
    bool isAudio = false,
    String? preferredHost,
  }) {
    if (!isEnabled) {
      return originalUrl;
    }

    if (!BtrRangeUtils.isBilibiliMediaUrl(originalUrl)) {
      return originalUrl;
    }

    // Ensure proxy server is up
    if (!_proxyServer.isRunning) {
      _proxyServer.start();
    }

    final port = _proxyServer.port;
    if (port == null) {
      // Proxy server not ready yet, gracefully fallback to original URL
      return originalUrl;
    }

    final sessionId = 's_${DateTime.now().microsecondsSinceEpoch}_${_sessionCounter++}_${isAudio ? "a" : "v"}';

    final session = BtrStreamSession(
      sessionId: sessionId,
      originalUrl: originalUrl,
      backupUrls: backupUrls.toList(),
      isAudio: isAudio,
      config: _config,
      resolver: _resolver,
      downloader: _downloader,
      preferredHost: preferredHost,
    );

    _proxyServer.registerSession(session);

    return 'http://127.0.0.1:$port/stream?id=$sessionId';
  }

  /// Clear all active sessions (e.g. when video changes or player disposes)
  void clearActiveSessions() {
    _proxyServer.clearSessions();
    BtrStats.instance.reset();
  }

  void dispose() {
    clearActiveSessions();
    _proxyServer.stop();
    _downloader.dispose();
  }
}
