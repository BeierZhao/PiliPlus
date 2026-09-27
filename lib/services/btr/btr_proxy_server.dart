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

  Future<void> start() async {
    if (_server != null) return;

    try {
      _server = await HttpServer.bind(InternetAddress.anyIPv4, 0, shared: true);
      _server!.autoCompress = false;
      _server!.listen(_handleRequest, onError: (e) {});
    } catch (_) {
      _server = null;
    }
  }

  void registerSession(BtrStreamSession session) {
    _sessions[session.sessionId] = session;
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
      if (sessionId == null || !_sessions.containsKey(sessionId)) {
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
        'nodes': snap.nodes.map((n) => {'host': n.host, 'state': n.state, 'speedBps': n.speedBps}).toList(),
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

    if (path == '/' || path == '/index.html') {
      final htmlContent = '''
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>BTR 竞速代理 - Android 独立网页测试工作台</title>
    <style>
        * { box-sizing: border-box; }
        body { background-color: #0f111a; color: #e0e6ed; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; margin: 0; padding: 16px; }
        .header { max-width: 960px; margin: 0 auto 16px; display: flex; align-items: center; justify-content: space-between; }
        .header h1 { color: #00a1d6; font-size: 18px; margin: 0; display: flex; align-items: center; gap: 8px; }
        .badge { background: rgba(0, 161, 214, 0.15); color: #00a1d6; border: 1px solid rgba(0, 161, 214, 0.3); font-size: 11px; padding: 2px 6px; border-radius: 4px; }
        
        .container { max-width: 960px; margin: 0 auto; display: grid; grid-template-columns: 1fr 320px; gap: 16px; }
        @media (max-width: 800px) { .container { grid-template-columns: 1fr; } }

        .card { background: #1a1d28; border: 1px solid #2a2e3d; border-radius: 12px; padding: 16px; box-shadow: 0 4px 16px rgba(0,0,0,0.4); }
        
        .search-box { display: flex; gap: 8px; margin-bottom: 16px; }
        .search-input { flex: 1; background: #0f111a; border: 1px solid #3a3f55; color: #fff; padding: 10px 14px; border-radius: 8px; font-size: 13px; outline: none; }
        .search-input:focus { border-color: #00a1d6; }
        .btn-parse { background: #00a1d6; color: #fff; border: none; padding: 10px 16px; border-radius: 8px; font-size: 13px; font-weight: 600; cursor: pointer; }
        .btn-parse:hover { background: #0088b5; }
        .btn-parse:disabled { background: #444; cursor: not-allowed; }

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
    </style>
</head>
<body>

    <div class="header">
        <h1>🚀 BTR 竞速代理 - 测试工作台 <span class="badge">Standalone Web Workbench</span></h1>
    </div>

    <div class="container">
        <!-- Left: Search & Video Player -->
        <div class="card">
            <div class="search-box">
                <input type="text" id="bvInput" class="search-input" value="BV1YHh26YEQp" placeholder="粘贴 B站视频链接或 BV 号 (如: BV1YHh26YEQp)...">
                <button id="loadBtn" class="btn-parse" onclick="loadVideo()">解析并播放</button>
            </div>

            <video id="player" controls autoplay name="media">
                <source id="videoSource" src="" type="video/mp4">
            </video>

            <div class="video-title" id="videoTitle">输入 B站视频链接即可开始独立测试</div>
            <div style="text-align: left;">
                <span class="status-pill"><span class="dot"></span> BTR 本地多线程代理服务正常运行中</span>
            </div>
        </div>

        <!-- Right: Realtime Monitor Dashboard -->
        <div class="card">
            <div class="dashboard-title">⚡ 实时内核监控面板</div>

            <div class="stat-grid">
                <div class="stat-item">
                    <div class="stat-label">活跃线程数</div>
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

            <div class="dashboard-title" style="margin-top: 16px;">🌐 CDN 节点接入与调度</div>
            <div id="nodesContainer" class="nodes-list">
                <div style="color: #666; font-size: 11px; text-align: center; padding: 15px;">暂未探测到活跃 CDN 节点</div>
            </div>
        </div>
    </div>

    <script>
        const statsUrl = '/stats';

        async function loadVideo() {
            const input = document.getElementById('bvInput').value.trim();
            if (!input) return;

            const btn = document.getElementById('loadBtn');
            btn.disabled = true;
            btn.innerText = '解析中...';

            try {
                const resp = await fetch('/api/parse?url=' + encodeURIComponent(input));
                const data = await resp.json();

                if (data.success) {
                    document.getElementById('videoTitle').innerText = data.title;
                    const player = document.getElementById('player');
                    const source = document.getElementById('videoSource');
                    source.src = data.streamUrl;
                    player.load();
                    player.play();
                } else {
                    alert('解析失败: ' + (data.error || '未知错误'));
                }
            } catch (e) {
                alert('网络请求失败: ' + e);
            } finally {
                btn.disabled = false;
                btn.innerText = '解析并播放';
            }
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
