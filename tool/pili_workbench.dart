import 'dart:async';
import 'dart:convert';
import 'dart:io';

import '../lib/services/btr/btr_cdn_resolver.dart';
import '../lib/services/btr/btr_config.dart';
import '../lib/services/btr/btr_idm_downloader.dart';
import '../lib/services/btr/btr_proxy_server.dart';
import '../lib/services/btr/btr_stats.dart';
import '../lib/services/btr/btr_stream_session.dart';

void main() async {
  print('================================================================');
  print('=== PILIPLUS REAL DASH VIDEO SIMULATION & PLAYBACK WORKBENCH ===');
  print('================================================================\n');

  final config = const BtrConfig(
    enabled: true,
    mode: BtrMode.mainland,
    concurrency: 8,
    firstByteTimeoutMs: 3500,
    hedgeDelayMs: 600,
  );

  final resolver = BtrCdnResolver();
  final downloader = BtrIdmDownloader(config: config, resolver: resolver);
  final proxyServer = BtrProxyServer();

  final logHistory = <String>[];
  void log(String msg) {
    final timestamp = DateTime.now().toIso8601String().substring(11, 19);
    final formatted = '[$timestamp] $msg';
    print(formatted);
    logHistory.add(formatted);
    if (logHistory.length > 100) logHistory.removeAt(0);
  }

  // Full PiliPlus DASH video parser (fnval=4048)
  Future<Map<String, dynamic>> parseDashVideo(String input) async {
    final client = HttpClient();
    client.connectionTimeout = const Duration(seconds: 8);

    try {
      String bvid = input.trim();
      final bvMatch = RegExp(r'BV[a-zA-Z0-9]+', caseSensitive: false).firstMatch(input);
      if (bvMatch != null) {
        bvid = bvMatch.group(0)!;
      }

      log('Parsing Bilibili video: $bvid (Exact PiliPlus DASH flow)...');

      final pageReq = await client.getUrl(Uri.parse('https://www.bilibili.com/video/$bvid'));
      pageReq.headers.set('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
      pageReq.headers.set('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8');

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
        return {'success': false, 'error': '无法从页面中提取 CID'};
      }

      log('Found CID: $cid, Title: "$title"');

      // Request DASH format (fnval=4048), exact same parameter as PiliPlus!
      final playUrl = 'https://api.bilibili.com/x/player/playurl?bvid=$bvid&cid=$cid&qn=80&fnval=4048';
      final playReq = await client.getUrl(Uri.parse(playUrl));
      playReq.headers.set('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
      playReq.headers.set('Referer', 'https://www.bilibili.com/video/$bvid');

      final playResp = await playReq.close();
      final playBody = await playResp.transform(utf8.decoder).join();
      final playJson = jsonDecode(playBody) as Map<String, dynamic>;

      if (playJson['code'] != 0) {
        return {'success': false, 'error': 'Bilibili API 错误: ${playJson['message']}'};
      }

      final data = playJson['data'];
      final dash = data['dash'];

      if (dash == null) {
        return {'success': false, 'error': '该视频未提供 DASH 流信息'};
      }

      final videoList = dash['video'] as List;
      final audioList = dash['audio'] as List?;

      final videoItem = videoList.first;
      final audioItem = (audioList != null && audioList.isNotEmpty) ? audioList.first : null;

      final rawVideoUrl = videoItem['baseUrl'] as String;
      final videoBackups = (videoItem['backupUrl'] as List?)?.cast<String>() ?? [];

      final rawAudioUrl = audioItem != null ? (audioItem['baseUrl'] as String) : '';
      final audioBackups = audioItem != null ? ((audioItem['backupUrl'] as List?)?.cast<String>() ?? []) : <String>[];

      final videoSessionId = 's_${DateTime.now().microsecondsSinceEpoch}_0_v';
      final audioSessionId = 's_${DateTime.now().microsecondsSinceEpoch}_1_a';

      // Register Video Session
      final vSession = BtrStreamSession(
        sessionId: videoSessionId,
        originalUrl: rawVideoUrl,
        backupUrls: videoBackups,
        isAudio: false,
        config: config,
        resolver: resolver,
        downloader: downloader,
      );
      proxyServer.registerSession(vSession);

      // Register Audio Session
      BtrStreamSession? aSession;
      if (rawAudioUrl.isNotEmpty) {
        aSession = BtrStreamSession(
          sessionId: audioSessionId,
          originalUrl: rawAudioUrl,
          backupUrls: audioBackups,
          isAudio: true,
          config: config,
          resolver: resolver,
          downloader: downloader,
        );
        proxyServer.registerSession(aSession);
      }

      // Simulate setDataSource retainSessions()
      final keepSet = {videoSessionId, if (rawAudioUrl.isNotEmpty) audioSessionId};
      proxyServer.retainSessions(keepSet);

      final videoStreamUrl = 'http://127.0.0.1:${proxyServer.port}/stream?id=$videoSessionId';
      final audioStreamUrl = rawAudioUrl.isNotEmpty ? 'http://127.0.0.1:${proxyServer.port}/stream?id=$audioSessionId' : '';

      final edlString = 'edl://!no_chapters;%${videoStreamUrl.length}%$videoStreamUrl;!new_stream;!no_chapters;%${audioStreamUrl.length}%$audioStreamUrl';

      log('Registered BTR Video Session: $videoSessionId');
      if (audioStreamUrl.isNotEmpty) {
        log('Registered BTR Audio Session: $audioSessionId');
      }

      return {
        'success': true,
        'bvid': bvid,
        'cid': cid,
        'title': title,
        'videoStreamUrl': videoStreamUrl,
        'audioStreamUrl': audioStreamUrl,
        'edlString': edlString,
        'rawVideoUrl': rawVideoUrl,
        'rawAudioUrl': rawAudioUrl,
      };
    } catch (e) {
      log('Error during video parse: $e');
      return {'success': false, 'error': '解析异常: $e'};
    } finally {
      client.close(force: true);
    }
  }

  proxyServer.parseBilibiliVideoHandler = parseDashVideo;

  // Custom handler for live logs and local player launcher
  proxyServer.customRequestHandler = (HttpRequest req) async {
    final path = req.uri.path;

    if (path == '/api/logs') {
      req.response.statusCode = HttpStatus.ok;
      req.response.headers.contentType = ContentType.json;
      req.response.write(jsonEncode({'logs': logHistory}));
      await req.response.close();
      return true;
    }

    if (path == '/api/launch_ffplay') {
      final streamUrl = req.uri.queryParameters['url'] ?? req.uri.queryParameters['video'];
      final type = req.uri.queryParameters['type'] ?? 'video';
      final windowTitle = type == 'audio'
          ? 'PiliPlus BTR 代理原生音频解码窗口 (FFplay)'
          : 'PiliPlus BTR 代理原生视频解码窗口 (FFplay)';

      if (streamUrl == null || streamUrl.isEmpty) {
        req.response.statusCode = HttpStatus.badRequest;
        req.response.write(jsonEncode({'success': false, 'error': 'Missing stream url parameter'}));
        await req.response.close();
        return true;
      }

      log('Launching local ffplay for $type: $streamUrl');
      try {
        if (Platform.isWindows) {
          Process.start('powershell.exe', [
            '-WindowStyle',
            'Normal',
            '-Command',
            'Start-Process ffplay -ArgumentList @("-window_title", "$windowTitle", "-autoexit", "$streamUrl")',
          ]);
        } else {
          Process.start('ffplay', [
            '-window_title',
            windowTitle,
            '-autoexit',
            streamUrl,
          ]);
        }
        req.response.statusCode = HttpStatus.ok;
        req.response.headers.contentType = ContentType.json;
        req.response.write(jsonEncode({'success': true}));
      } catch (e) {
        log('Failed to start ffplay: $e');
        req.response.statusCode = HttpStatus.internalServerError;
        req.response.headers.contentType = ContentType.json;
        req.response.write(jsonEncode({'success': false, 'error': '$e'}));
      }
      await req.response.close();
      return true;
    }

    return false;
  };

  await proxyServer.start(preferredPort: 8341);
  final port = proxyServer.port!;

  print('================================================================');
  print('🚀 PILIPLUS BTR 验证工作台已在本地启动: http://127.0.0.1:$port');
  print('👉 浏览器打开 http://127.0.0.1:$port 即可实测完整 PiliPlus DASH 视频流播放！');
  print('================================================================\n');

  // Keep process alive indefinitely
  final completer = Completer<void>();
  ProcessSignal.sigint.watch().listen((_) {
    print('\nStopping workbench...');
    proxyServer.stop();
    downloader.dispose();
    completer.complete();
    exit(0);
  });

  await completer.future;
}
