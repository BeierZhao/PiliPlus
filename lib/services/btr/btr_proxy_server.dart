import 'dart:convert';
import 'dart:io';

import 'btr_stats.dart';
import 'btr_stream_session.dart';

class BtrProxyServer {
  HttpServer? _server;
  final Map<String, BtrStreamSession> _sessions = {};

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

    if (path == '/' || path == '/index.html') {
      request.response.statusCode = HttpStatus.ok;
      request.response.headers.contentType = ContentType.html;
      request.response.write('<html><body style="background:#121212;color:#fff;font-family:sans-serif;text-align:center;padding:50px;">'
          '<h1 style="color:#00a1d6">🚀 BTR Proxy Server Running</h1>'
          '<p>Active Sessions: ${_sessions.length}</p>'
          '<p><a href="/stats" style="color:#00a1d6">View JSON Stats (/stats)</a></p>'
          '</body></html>');
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
