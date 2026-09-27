import 'dart:convert';
import 'dart:io';

import 'btr_stats.dart';
import 'btr_stream_session.dart';

class BtrProxyServer {
  HttpServer? _server;
  final Map<String, BtrStreamSession> _sessions = {};
  Future<Map<String, dynamic>> Function(String url)? parseBilibiliVideoHandler;

  bool get isRunning => _server != null;

  int? get port => _server?.port;

  String? get baseUrl => isRunning ? 'http://127.0.0.1:$port' : null;

  Future<void> start({int preferredPort = 8341}) async {
    if (_server != null) return;

    try {
      _server = await HttpServer.bind(InternetAddress.anyIPv4, preferredPort, shared: true);
      _server!.autoCompress = false;
      _server!.listen(_handleRequest, onError: (e) {});
    } catch (_) {
      try {
        _server = await HttpServer.bind(InternetAddress.anyIPv4, 0, shared: true);
        _server!.autoCompress = false;
        _server!.listen(_handleRequest, onError: (e) {});
      } catch (_) {
        _server = null;
      }
    }
  }

  void registerSession(BtrStreamSession session) {
    if (_sessions.length >= 30) {
      final oldestId = _sessions.keys.first;
      _sessions.remove(oldestId)?.cancelAll();
    }
    _sessions[session.sessionId] = session;
  }

  void retainSessions(Set<String> keepSessionIds) {
    if (keepSessionIds.isEmpty) {
      clearSessions();
      return;
    }
    // Retain up to 20 recent sessions to prevent premature teardown during navigation
    while (_sessions.length > 20) {
      final candidateId = _sessions.keys.firstWhere(
        (id) => !keepSessionIds.contains(id),
        orElse: () => '',
      );
      if (candidateId.isEmpty) break;
      _sessions.remove(candidateId)?.cancelAll();
    }
  }

  void removeSession(String sessionId) {
    _sessions.remove(sessionId)?.cancelAll();
  }

  void clearSessions() {
    for (final session in _sessions.values) {
      session.cancelAll();
    }
    _sessions.clear();
  }

  Future<bool> Function(HttpRequest request)? customRequestHandler;

  Future<void> _handleRequest(HttpRequest request) async {
    request.response.headers.set('Access-Control-Allow-Origin', '*');
    request.response.headers.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
    request.response.headers.set('Access-Control-Allow-Headers', '*');
    try {
      request.response.headers.removeAll('X-Frame-Options');
    } catch (_) {}

    if (request.method.toUpperCase() == 'OPTIONS') {
      request.response.statusCode = HttpStatus.noContent;
      await request.response.close();
      return;
    }

    if (customRequestHandler != null) {
      final handled = await customRequestHandler!(request);
      if (handled) return;
    }

    final path = request.uri.path;

    if (path == '/stream') {
      final sessionId = request.uri.queryParameters['id'];
      final range = request.headers.value(HttpHeaders.rangeHeader);
      print('[BTR Proxy] Incoming ${request.method} /stream (id: $sessionId, Range: $range)');
      if (sessionId == null || !_sessions.containsKey(sessionId)) {
        print('[BTR Proxy] Session not found or expired: $sessionId (active: ${_sessions.keys.toList()})');
        request.response.statusCode = HttpStatus.notFound;
        request.response.write('Session not found or expired');
        await request.response.close();
        return;
      }

      final session = _sessions[sessionId]!;
      await session.handleHttpRequest(request);
      return;
    }

    if (path == '/stats') {
      final snap = BtrStats.instance.snapshot;
      request.response.statusCode = HttpStatus.ok;
      request.response.headers.contentType = ContentType.json;
      request.response.write(jsonEncode({
        'downloadSpeedBps': snap.downloadSpeedBps,
        'speedFormatted': snap.speedFormatted,
        'activeConnections': snap.activeConnections,
        'rescuedChunks': snap.rescuedChunks,
        'totalBytes': snap.totalBytes,
        'currentPrimaryHost': snap.currentPrimaryHost,
        'nodes': snap.nodes.map((n) => {'host': n.host, 'state': n.state, 'speedBps': n.speedBps}).toList(),
        'activeThreads': snap.activeThreads.map((t) => {
          'threadId': t.threadId,
          'host': t.host,
          'isAudio': t.isAudio,
          'chunkIndex': t.chunkIndex,
          'speedFormatted': t.speedFormatted,
          'speedBps': t.speedBps,
          'progress': t.progress,
        }).toList(),
      }));
      await request.response.close();
      return;
    }

    if (path == '/api/parse') {
      final inputUrl = request.uri.queryParameters['url'] ?? '';
      final parseRes = parseBilibiliVideoHandler != null
          ? await parseBilibiliVideoHandler!(inputUrl)
          : {'success': false, 'error': 'Parser handler not registered'};
      request.response.statusCode = HttpStatus.ok;
      request.response.headers.contentType = ContentType.json;
      request.response.write(jsonEncode(parseRes));
      await request.response.close();
      return;
    }

    if (path == '/api/launch_ffplay') {
      final streamUrl = request.uri.queryParameters['url'] ?? request.uri.queryParameters['video'];
      final type = request.uri.queryParameters['type'] ?? 'video';
      final windowTitle = type == 'audio'
          ? 'BTR 代理原生音频解码窗口 (FFplay)'
          : 'BTR 代理原生视频解码窗口 (FFplay)';

      if (streamUrl == null || streamUrl.isEmpty) {
        request.response.statusCode = HttpStatus.badRequest;
        request.response.write(jsonEncode({'success': false, 'error': 'Missing stream url parameter'}));
        await request.response.close();
        return;
      }
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
        request.response.statusCode = HttpStatus.ok;
        request.response.headers.contentType = ContentType.json;
        request.response.write(jsonEncode({'success': true}));
      } catch (e) {
        request.response.statusCode = HttpStatus.internalServerError;
        request.response.headers.contentType = ContentType.json;
        request.response.write(jsonEncode({'success': false, 'error': '$e'}));
      }
      await request.response.close();
      return;
    }

    if (path == '/' || path == '/index.html') {
      final htmlContent = '''
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>BTR 竞速代理 - PiliPlus 完整视频流实测工作台</title>
    <style>
        * { box-sizing: border-box; }
        body { background-color: #0f111a; color: #e0e6ed; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; margin: 0; padding: 16px; }
        .header { max-width: 1000px; margin: 0 auto 16px; display: flex; align-items: center; justify-content: space-between; }
        .header h1 { color: #00a1d6; font-size: 18px; margin: 0; display: flex; align-items: center; gap: 8px; }
        .badge { background: rgba(0, 161, 214, 0.15); color: #00a1d6; border: 1px solid rgba(0, 161, 214, 0.3); font-size: 11px; padding: 2px 6px; border-radius: 4px; }
        
        .container { max-width: 1000px; margin: 0 auto; display: grid; grid-template-columns: 1fr 340px; gap: 16px; }
        @media (max-width: 860px) { .container { grid-template-columns: 1fr; } }

        .card { background: #1a1d28; border: 1px solid #2a2e3d; border-radius: 12px; padding: 16px; box-shadow: 0 4px 16px rgba(0,0,0,0.4); margin-bottom: 16px; }
        
        .search-box { display: flex; gap: 8px; margin-bottom: 16px; }
        .search-input { flex: 1; background: #0f111a; border: 1px solid #3a3f55; color: #fff; padding: 10px 14px; border-radius: 8px; font-size: 13px; outline: none; }
        .search-input:focus { border-color: #00a1d6; }
        .btn-parse { background: #00a1d6; color: #fff; border: none; padding: 10px 16px; border-radius: 8px; font-size: 13px; font-weight: 600; cursor: pointer; transition: 0.2s; }
        .btn-parse:hover { background: #0088b5; }
        .btn-parse:disabled { background: #444; cursor: not-allowed; }

        .btn-secondary { background: #2f3547; color: #a0aec0; border: 1px solid #3e465e; padding: 8px 12px; border-radius: 6px; font-size: 12px; cursor: pointer; transition: 0.2s; display: inline-flex; align-items: center; gap: 6px; }
        .btn-secondary:hover { background: #3e465e; color: #fff; }

        video { width: 100%; height: auto; border-radius: 8px; background: #000; display: block; outline: none; margin-bottom: 12px; }

        .video-title { font-size: 15px; font-weight: 600; color: #fff; margin: 0 0 8px; text-align: left; }
        .status-pill { display: inline-flex; align-items: center; gap: 6px; background: #232736; padding: 5px 10px; border-radius: 20px; font-size: 11px; color: #4caf50; }
        .dot { width: 6px; height: 6px; background: #4caf50; border-radius: 50%; display: inline-block; }

        .dashboard-title { font-size: 14px; font-weight: 600; color: #fff; margin: 0 0 12px; padding-bottom: 8px; border-bottom: 1px solid #2a2e3d; text-align: left; }
        .stat-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 16px; }
        .stat-item { background: #0f111a; padding: 10px; border-radius: 8px; border: 1px solid #252938; text-align: left; }
        .stat-label { font-size: 10px; color: #8892b0; margin-bottom: 2px; }
        .stat-value { font-size: 16px; font-weight: 700; color: #fff; font-family: monospace; }
        .stat-value.highlight { color: #00a1d6; }
        .stat-value.green { color: #4caf50; }

        .nodes-list { text-align: left; }
        .node-card { background: #0f111a; border: 1px solid #252938; padding: 8px 10px; border-radius: 6px; margin-bottom: 6px; font-family: monospace; font-size: 11px; display: flex; justify-content: space-between; align-items: center; }
        .node-host { color: #ccd6f6; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 170px; }
        .node-tag { padding: 2px 5px; border-radius: 3px; font-size: 9px; font-weight: bold; }
        .tag-healthy { background: rgba(76, 175, 80, 0.2); color: #4caf50; }
        .tag-blocked { background: rgba(244, 67, 54, 0.2); color: #f44336; }
        .tag-active { background: rgba(0, 161, 214, 0.2); color: #00a1d6; }

        .stream-info-box { background: #0f111a; border: 1px solid #252938; border-radius: 8px; padding: 12px; margin-top: 12px; text-align: left; font-size: 11px; font-family: monospace; }
        .stream-info-row { margin-bottom: 6px; word-break: break-all; color: #a0aec0; }
        .stream-info-row b { color: #00a1d6; }
    </style>
</head>
<body>

    <div class="header">
        <h1>🚀 BTR 竞速代理 - PiliPlus 完整视频流实测工作台 <span class="badge">DASH Dual-Stream Verified</span></h1>
        <div>
            <span class="status-pill"><span class="dot"></span> 代理服务运行于 127.0.0.1:$port</span>
        </div>
    </div>

    <div class="container">
        <!-- Left: Search & Video Player -->
        <div>
            <div class="card">
                <div class="search-box">
                    <input type="text" id="bvInput" class="search-input" value="BV1YHh26YEQp" placeholder="输入任意 B站视频链接或 BV 号 (如: BV1YHh26YEQp)...">
                    <button id="loadBtn" class="btn-parse" onclick="loadVideo()">解析并播放 (完整流程)</button>
                </div>

                <!-- Video Element with hidden audio sync element for real DASH audio/video playback -->
                <video id="player" controls autoplay name="media">
                    <source id="videoSource" src="" type="video/mp4">
                </video>
                <audio id="audioPlayer" preload="auto"></audio>

                <div class="video-title" id="videoTitle">输入 B站视频链接即可开始独立测试</div>

                <div style="display: flex; gap: 8px; margin-top: 12px; flex-wrap: wrap;">
                    <button class="btn-secondary" onclick="launchFFplay('video')">
                        📺 启动本地窗口 (视频轨 FFplay)
                    </button>
                    <button class="btn-secondary" onclick="launchFFplay('audio')">
                        🎵 启动本地窗口 (音频轨 FFplay)
                    </button>
                    <button class="btn-secondary" onclick="copyEdl()">
                        📋 复制 PiliPlus EDL 播放列表
                    </button>
                </div>
                <div style="font-size: 11px; color: #8892b0; margin-top: 8px; text-align: left;">
                    💡 提示：网页端已实现 DASH 纯音视频双流自动同步。若浏览器策略限制了自动发声，直接点击上方画面中的“播放 ▶️”即可。
                </div>

                <div class="stream-info-box" id="streamInfoBox" style="display: none;">
                    <div class="stream-info-row"><b>视频 DASH 流 (BTR 代理):</b> <span id="vStreamUrl"></span></div>
                    <div class="stream-info-row"><b>音频 DASH 流 (BTR 代理):</b> <span id="aStreamUrl"></span></div>
                    <div class="stream-info-row"><b>PiliPlus 播放参数 (EDL):</b> <span id="edlText"></span></div>
                </div>
            </div>
        </div>

        <!-- Right: Realtime Monitor Dashboard -->
        <div>
            <div class="card">
                <div class="dashboard-title">⚡ BTR 竞速内核实时监控</div>

                <div class="stat-grid">
                    <div class="stat-item">
                        <div class="stat-label">活跃并发连接</div>
                        <div class="stat-value highlight" id="activeThreads">0 / 8</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">实时下载速率</div>
                        <div class="stat-value green" id="downloadSpeed">0 KB/s</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">卡顿救援次数</div>
                        <div class="stat-value" id="rescuedCount">0</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">已传输数据</div>
                        <div class="stat-value" id="totalBytes">0 MB</div>
                    </div>
                </div>

                <div class="dashboard-title" style="margin-top: 16px;">🌐 候选 CDN 节点健康与调度</div>
                <div id="nodesContainer" class="nodes-list">
                    <div style="color: #666; font-size: 11px; text-align: center; padding: 15px;">暂未探测到活跃 CDN 节点</div>
                </div>
            </div>
        </div>
    </div>

    <script>
        const statsUrl = '/stats';
        let currentParseData = null;

        const player = document.getElementById('player');
        const audioPlayer = document.getElementById('audioPlayer');

        // Synchronize Video and Audio playback for real DASH streaming
        player.onplay = () => { if (audioPlayer.src) audioPlayer.play(); };
        player.onpause = () => { if (audioPlayer.src) audioPlayer.pause(); };
        player.onseeking = () => { if (audioPlayer.src) audioPlayer.currentTime = player.currentTime; };
        player.onseeked = () => { if (audioPlayer.src) audioPlayer.currentTime = player.currentTime; };
        player.onratechange = () => { if (audioPlayer.src) audioPlayer.playbackRate = player.playbackRate; };
        player.onvolumechange = () => { if (audioPlayer.src) audioPlayer.volume = player.muted ? 0 : player.volume; };

        async function loadVideo() {
            const input = document.getElementById('bvInput').value.trim();
            if (!input) return;

            const btn = document.getElementById('loadBtn');
            btn.disabled = true;
            btn.innerText = '正在按 PiliPlus 完整流程解析 DASH 流...';

            try {
                const resp = await fetch('/api/parse?url=' + encodeURIComponent(input));
                const data = await resp.json();
                currentParseData = data;

                if (data.success) {
                    document.getElementById('videoTitle').innerText = data.title;
                    const vUrl = data.videoStreamUrl || data.streamUrl;
                    const aUrl = data.audioStreamUrl || '';

                    // Load Video
                    document.getElementById('videoSource').src = vUrl;
                    player.load();
                    player.play().catch(() => {});

                    // Load Audio
                    if (aUrl) {
                        audioPlayer.src = aUrl;
                        audioPlayer.load();
                        audioPlayer.play().catch(() => {});
                    } else {
                        audioPlayer.src = '';
                    }

                    // Show Stream Info
                    document.getElementById('streamInfoBox').style.display = 'block';
                    document.getElementById('vStreamUrl').innerText = vUrl;
                    document.getElementById('aStreamUrl').innerText = aUrl || '无独立音轨 (包含在视频流中)';
                    document.getElementById('edlText').innerText = data.edlString || vUrl;
                } else {
                    alert('解析失败: ' + (data.error || '未知错误'));
                }
            } catch (e) {
                alert('网络请求失败: ' + e);
            } finally {
                btn.disabled = false;
                btn.innerText = '解析并播放 (完整流程)';
            }
        }

        async function launchFFplay(type = 'video') {
            if (!currentParseData) {
                alert('请先点击“解析并播放”成功获取视频流');
                return;
            }
            const targetUrl = type === 'audio' ? currentParseData.audioStreamUrl : currentParseData.videoStreamUrl;
            if (!targetUrl) {
                alert('未找到对应的流地址 (可能无独立音轨)');
                return;
            }
            try {
                const resp = await fetch('/api/launch_ffplay?url=' + encodeURIComponent(targetUrl) + '&type=' + encodeURIComponent(type));
                const res = await resp.json();
                if (res.success) {
                    console.log('FFplay 启动成功: ' + type);
                } else {
                    alert('启动 FFplay 失败: ' + res.error);
                }
            } catch (e) {
                alert('请求异常: ' + e);
            }
        }

        function copyEdl() {
            if (!currentParseData || !currentParseData.edlString) {
                alert('暂无 EDL 播放列表');
                return;
            }
            navigator.clipboard.writeText(currentParseData.edlString);
            alert('PiliPlus EDL 播放列表已复制到剪贴板！');
        }

        async function updateStats() {
            try {
                const resp = await fetch(statsUrl);
                const stats = await resp.json();

                document.getElementById('activeThreads').innerText = stats.activeConnections + ' / ' + stats.maxConcurrency;
                document.getElementById('downloadSpeed').innerText = stats.speedFormatted;
                document.getElementById('rescuedCount').innerText = stats.rescuedChunks;
                document.getElementById('totalBytes').innerText = (stats.totalBytes / (1024 * 1024)).toFixed(2) + ' MB';

                const nodesContainer = document.getElementById('nodesContainer');
                if (stats.nodes && stats.nodes.length > 0) {
                    nodesContainer.innerHTML = stats.nodes.map(n => {
                        const speedText = n.speedBps > 1024 * 1024 
                            ? (n.speedBps / (1024 * 1024)).toFixed(1) + ' MB/s'
                            : (n.speedBps / 1024).toFixed(0) + ' KB/s';
                        
                        let tagClass = 'tag-healthy';
                        let tagText = '正常';
                        if (n.state === 'blocked' || n.state === 'banned') {
                            tagClass = 'tag-blocked';
                            tagText = '熔断';
                        } else if (n.speedBps > 0) {
                            tagClass = 'tag-active';
                            tagText = '活跃 (' + speedText + ')';
                        }

                        return '<div class="node-card">' +
                                '<span class="node-host" title="' + n.host + '">' + n.host + '</span>' +
                                '<span class="node-tag ' + tagClass + '">' + tagText + '</span>' +
                               '</div>';
                    }).join('');
                } else {
                    nodesContainer.innerHTML = '<div style="color: #666; font-size: 11px; text-align: center; padding: 10px;">暂未探测到活跃 CDN 节点</div>';
                }
            } catch (e) {}
        }

        setInterval(updateStats, 1000);
        updateStats();

        // Auto load initial BV if present in input
        loadVideo();
    </script>
</body>
</html>
''';
      request.response.statusCode = HttpStatus.ok;
      request.response.headers.contentType = ContentType.html;
      request.response.write(htmlContent);
      await request.response.close();
      return;
    }

    request.response.statusCode = HttpStatus.notFound;
    await request.response.close();
  }

  Future<void> stop() async {
    clearSessions();
    await _server?.close(force: true);
    _server = null;
  }
}
