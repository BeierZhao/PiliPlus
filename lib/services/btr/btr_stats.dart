import 'dart:async';

class BtrNodeStat {
  final String host;
  final String state; // 'healthy', 'blocked', 'banned', 'untested'
  final double speedBps;

  const BtrNodeStat({
    required this.host,
    required this.state,
    required this.speedBps,
  });
}

class BtrSnapshot {
  final double downloadSpeedBps;
  final int activeConnections;
  final int maxConcurrency;
  final int rescuedChunks;
  final int totalBytes;
  final List<BtrNodeStat> nodes;

  const BtrSnapshot({
    required this.downloadSpeedBps,
    required this.activeConnections,
    required this.maxConcurrency,
    required this.rescuedChunks,
    required this.totalBytes,
    required this.nodes,
  });

  String get speedFormatted {
    final speed = downloadSpeedBps;
    if (speed >= 1024 * 1024) {
      return '${(speed / (1024 * 1024)).toStringAsFixed(2)} MB/s';
    } else if (speed >= 1024) {
      return '${(speed / 1024).toStringAsFixed(1)} KB/s';
    } else {
      return '${speed.toStringAsFixed(0)} B/s';
    }
  }
}

class BtrStats {
  static final BtrStats instance = BtrStats._();
  BtrStats._();

  int _activeConnections = 0;
  int _maxConcurrency = 8;
  int _rescuedChunks = 0;
  int _totalDownloadedBytes = 0;

  final List<({int timestamp, int bytes})> _speedBuckets = [];
  static const int _speedWindowMs = 2000;

  final StreamController<BtrSnapshot> _streamController = StreamController<BtrSnapshot>.broadcast();
  Stream<BtrSnapshot> get stream => _streamController.stream;

  final Map<String, ({String state, double bps})> _nodeHealth = {};

  int get activeConnections => _activeConnections;
  int get rescuedChunks => _rescuedChunks;
  int get totalDownloadedBytes => _totalDownloadedBytes;

  void setMaxConcurrency(int limit) {
    _maxConcurrency = limit;
    _notify();
  }

  void onConnectionStarted() {
    _activeConnections++;
    _notify();
  }

  void onConnectionClosed() {
    if (_activeConnections > 0) _activeConnections--;
    _notify();
  }

  void onBytesReceived(int bytes) {
    if (bytes <= 0) return;
    _totalDownloadedBytes += bytes;
    final now = DateTime.now().millisecondsSinceEpoch;
    _speedBuckets.add((timestamp: now, bytes: bytes));
    _pruneBuckets(now);
    _notify();
  }

  void onHedgeRescue() {
    _rescuedChunks++;
    _notify();
  }

  void updateNodeStatus(String host, String state, double bps) {
    _nodeHealth[host] = (state: state, bps: bps);
    _notify();
  }

  void _pruneBuckets(int now) {
    while (_speedBuckets.isNotEmpty && now - _speedBuckets.first.timestamp > _speedWindowMs) {
      _speedBuckets.removeAt(0);
    }
  }

  double get currentSpeedBps {
    final now = DateTime.now().millisecondsSinceEpoch;
    _pruneBuckets(now);
    if (_speedBuckets.isEmpty) return 0.0;
    final totalBytes = _speedBuckets.fold<int>(0, (sum, item) => sum + item.bytes);
    final spanMs = now - _speedBuckets.first.timestamp;
    if (spanMs <= 0) return totalBytes.toDouble();
    return totalBytes * 1000.0 / spanMs;
  }

  BtrSnapshot get snapshot {
    final nodes = _nodeHealth.entries.map((e) {
      return BtrNodeStat(host: e.key, state: e.value.state, speedBps: e.value.bps);
    }).toList();

    return BtrSnapshot(
      downloadSpeedBps: currentSpeedBps,
      activeConnections: _activeConnections,
      maxConcurrency: _maxConcurrency,
      rescuedChunks: _rescuedChunks,
      totalBytes: _totalDownloadedBytes,
      nodes: nodes,
    );
  }

  void _notify() {
    if (_streamController.hasListener) {
      _streamController.add(snapshot);
    }
  }

  void reset() {
    _activeConnections = 0;
    _rescuedChunks = 0;
    _totalDownloadedBytes = 0;
    _speedBuckets.clear();
    _nodeHealth.clear();
    _notify();
  }

  void dispose() {
    _streamController.close();
  }
}
