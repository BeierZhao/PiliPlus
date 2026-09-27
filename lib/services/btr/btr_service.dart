import 'dart:async';
import 'dart:convert';
import 'dart:io';
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
    _proxyServer = BtrProxyServer()
      ..parseBilibiliVideoHandler = parseBilibiliVideo;

    if (isEnabled) {
      unawaited(_proxyServer.start().then((_) {
        print('[BTR] Proxy Server Test Workbench: http://127.0.0.1:${_proxyServer.port}/');
      }));
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

  /// Extract session IDs from media source URLs or EDL playlists
  Set<String> extractSessionIds(String? source) {
    if (source == null || source.isEmpty) return const {};
    final regex = RegExp(r'[?&]id=([a-zA-Z0-9_]+)');
    final matches = regex.allMatches(source);
    return matches.map((m) => m.group(1)!).toSet();
  }

  /// Retain specified sessions and cleanup older unused sessions
  void retainSessions(Set<String> keepSessionIds) {
    _proxyServer.retainSessions(keepSessionIds);
  }

  /// Clear all active sessions (e.g. when video changes or player disposes)
  void clearActiveSessions() {
    _proxyServer.clearSessions();
    BtrStats.instance.reset();
  }

  /// Parse Bilibili Video URL or BV ID for standalone Web Workbench testing
  Future<Map<String, dynamic>> parseBilibiliVideo(String input) async {
    final client = HttpClient();
    client.connectionTimeout = const Duration(seconds: 8);

    try {
      String bvid = input.trim();
      final bvMatch = RegExp(r'BV[a-zA-Z0-9]+', caseSensitive: false).firstMatch(input);
      if (bvMatch != null) {
        bvid = bvMatch.group(0)!;
      }

      final pageReq = await client.getUrl(Uri.parse('https://www.bilibili.com/video/$bvid'));
      pageReq.headers.set('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
      pageReq.headers.set('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8');

      final pageResp = await pageReq.close();
      final pageHtml = await pageResp.transform(utf8.decoder).join();

      int cid = 0;
      String title = 'Bilibili Video ($bvid)';

      final cidMatch = RegExp(r'"cid"\s*:\s*(\d+)').firstMatch(pageHtml);
      if (cidMatch != null) {
        cid = int.parse(cidMatch.group(1)!);
      }

      final titleMatch = RegExp(r'<title[^>]*>(.*?)</title>', caseSensitive: false).firstMatch(pageHtml);
      if (titleMatch != null) {
        title = titleMatch.group(1)!.replaceAll('_哔哩哔哩_bilibili', '').trim();
      }

      if (cid == 0) {
        return {'success': false, 'error': '无法解析该视频的 CID'};
      }

      final playReq = await client.getUrl(Uri.parse('https://api.bilibili.com/x/player/playurl?bvid=$bvid&cid=$cid&qn=64&type=mp4&platform=html5'));
      playReq.headers.set('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
      playReq.headers.set('Referer', 'https://www.bilibili.com/video/$bvid');

      final playResp = await playReq.close();
      final playBody = await playResp.transform(utf8.decoder).join();
      final playJson = jsonDecode(playBody);

      if (playJson['code'] != 0 || playJson['data']['durl'] == null) {
        return {'success': false, 'error': 'B站 Playurl API 报错: ${playJson['message']}'};
      }

      final durl = playJson['data']['durl'][0];
      final videoUrl = durl['url'] as String;
      final backupUrls = (durl['backup_url'] as List?)?.cast<String>() ?? [];

      final streamUrl = wrapUrl(
        originalUrl: videoUrl,
        backupUrls: backupUrls,
        isAudio: false,
      );

      return {
        'success': true,
        'bvid': bvid,
        'title': title,
        'cid': cid,
        'streamUrl': streamUrl,
        'originalUrl': videoUrl,
      };
    } catch (e) {
      return {'success': false, 'error': '解析异常: $e'};
    } finally {
      client.close(force: true);
    }
  }

  void dispose() {
    clearActiveSessions();
    _proxyServer.stop();
    _downloader.dispose();
  }
}
