import 'dart:async';
import 'dart:math';

class BtrThreadStat {
  final int threadId;
  final String host;
  final bool isAudio;
  final int chunkIndex;
  final int start;
  final int end;
  final int receivedBytes;
  final double speedBps;
  final int startedAt;

  const BtrThreadStat({
    required this.threadId,
    required this.host,
    required this.isAudio,
    required this.chunkIndex,
    required this.start,
    required this.end,
    required this.receivedBytes,
    required this.speedBps,
    required this.startedAt,
  });

  int get totalBytes => end >= start ? end - start + 1 : 0;
  double get progress => totalBytes > 0 ? (receivedBytes / totalBytes).clamp(0.0, 1.0) : 0.0;

  String get speedFormatted {
    if (speedBps >= 1024 * 1024) {
      return '${(speedBps / (1024 * 1024)).toStringAsFixed(2)} MB/s';
    } else if (speedBps >= 1024) {
      return '${(speedBps / 1024).toStringAsFixed(1)} KB/s';
    } else {
      return '${speedBps.toStringAsFixed(0)} B/s';
    }
  }

  String get receivedFormatted {
    final bytes = receivedBytes;
    if (bytes >= 1024 * 1024) {
      return '${(bytes / (1024 * 1024)).toStringAsFixed(2)} MB';
    } else if (bytes >= 1024) {
      return '${(bytes / 1024).toStringAsFixed(1)} KB';
    } else {
      return '$bytes B';
    }
  }

  String get totalFormatted {
    final bytes = totalBytes;
    if (bytes >= 1024 * 1024) {
      return '${(bytes / (1024 * 1024)).toStringAsFixed(2)} MB';
    } else if (bytes >= 1024) {
      return '${(bytes / 1024).toStringAsFixed(1)} KB';
    } else {
      return '$bytes B';
    }
  }
}

class BtrNodeStat {
  final String host;
  final String state; // 'healthy', 'blocked', 'banned', 'untested'
  final double speedBps;
  final bool isPrimary;
  final bool isCandidate;

  const BtrNodeStat({
    required this.host,
    required this.state,
    required this.speedBps,
    this.isPrimary = false,
    this.isCandidate = false,
  });

  String get speedFormatted {
    if (speedBps >= 1024 * 1024) {
      return '${(speedBps / (1024 * 1024)).toStringAsFixed(2)} MB/s';
    } else if (speedBps >= 1024) {
      return '${(speedBps / 1024).toStringAsFixed(1)} KB/s';
    } else if (speedBps > 0) {
      return '${speedBps.toStringAsFixed(0)} B/s';
    } else {
      return '--';
    }
  }
}

class BtrSnapshot {
  final double downloadSpeedBps;
  final int activeConnections;
  final int maxConcurrency;
  final int rescuedChunks;
  final int totalBytes;
  final List<BtrNodeStat> nodes;
  final List<BtrThreadStat> activeThreads;
  final List<BtrThreadStat> recentThreads;
  final String? currentPrimaryHost;
  final List<String> candidateHosts;

  const BtrSnapshot({
    required this.downloadSpeedBps,
    required this.activeConnections,
    required this.maxConcurrency,
    required this.rescuedChunks,
    required this.totalBytes,
    required this.nodes,
    this.activeThreads = const [],
    this.recentThreads = const [],
    this.currentPrimaryHost,
    this.candidateHosts = const [],
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

class _ThreadTracker {
  final int id;
  final String host;
  final bool isAudio;
  final int chunkIndex;
  final int start;
  final int end;
  int receivedBytes = 0;
  double speedBps = 0.0;
  final int startedAt;
  int lastUpdateMs;
  int lastBytes = 0;

  _ThreadTracker({
    required this.id,
    required this.host,
    required this.isAudio,
    required this.chunkIndex,
    required this.start,
    required this.end,
    required this.startedAt,
    required this.lastUpdateMs,
  });

  BtrThreadStat toStat() => BtrThreadStat(
    threadId: id,
    host: host,
    isAudio: isAudio,
    chunkIndex: chunkIndex,
    start: start,
    end: end,
    receivedBytes: receivedBytes,
    speedBps: speedBps,
    startedAt: startedAt,
  );
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

  int _nextThreadId = 1;
  final Map<int, _ThreadTracker> _activeThreads = {};
  final List<BtrThreadStat> _recentThreads = [];
  String? _primaryHost;
  final Set<String> _candidateHosts = {};

  int get activeConnections => _activeConnections;
  int get rescuedChunks => _rescuedChunks;
  int get totalDownloadedBytes => _totalDownloadedBytes;
  String? get currentPrimaryHost => _primaryHost;
  List<String> get candidateHosts => _candidateHosts.toList();

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

  int onThreadStarted({
    required String host,
    required bool isAudio,
    required int chunkIndex,
    required int start,
    required int end,
  }) {
    final now = DateTime.now().millisecondsSinceEpoch;
    final id = _nextThreadId++;
    _activeThreads[id] = _ThreadTracker(
      id: id,
      host: host,
      isAudio: isAudio,
      chunkIndex: chunkIndex,
      start: start,
      end: end,
      startedAt: now,
      lastUpdateMs: now,
    );
    _activeConnections = _activeThreads.length;
    _notify();
    return id;
  }

  void onThreadBytesReceived(int threadId, int bytes) {
    if (bytes <= 0) return;
    onBytesReceived(bytes);
    final tracker = _activeThreads[threadId];
    if (tracker == null) return;
    tracker.receivedBytes += bytes;
    final now = DateTime.now().millisecondsSinceEpoch;
    final dt = now - tracker.lastUpdateMs;
    if (dt >= 200) {
      final deltaBytes = tracker.receivedBytes - tracker.lastBytes;
      final instBps = deltaBytes * 1000.0 / dt;
      tracker.speedBps = tracker.speedBps > 0 ? (tracker.speedBps * 0.3 + instBps * 0.7) : instBps;
      tracker.lastUpdateMs = now;
      tracker.lastBytes = tracker.receivedBytes;
    }
  }

  void onThreadClosed(int threadId, {double? finalSpeedBps}) {
    final tracker = _activeThreads.remove(threadId);
    if (tracker != null) {
      final now = DateTime.now().millisecondsSinceEpoch;
      final totalElapsed = max(1, now - tracker.startedAt);
      final avgBps = finalSpeedBps ?? (tracker.receivedBytes * 1000.0 / totalElapsed);
      tracker.speedBps = avgBps;
      _recentThreads.insert(0, tracker.toStat());
      if (_recentThreads.length > 8) {
        _recentThreads.removeLast();
      }
    }
    _activeConnections = _activeThreads.length;
    _notify();
  }

  void updateSessionInfo({String? primaryHost, List<String>? candidateHosts}) {
    if (primaryHost != null && primaryHost.isNotEmpty) _primaryHost = primaryHost;
    if (candidateHosts != null) {
      _candidateHosts.addAll(candidateHosts.where((h) => h.isNotEmpty));
    }
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
    final allHosts = <String>{
      ..._nodeHealth.keys,
      if (_primaryHost != null) _primaryHost!,
      ..._candidateHosts,
    };

    final nodes = allHosts.map((host) {
      final health = _nodeHealth[host];
      return BtrNodeStat(
        host: host,
        state: health?.state ?? 'untested',
        speedBps: health?.bps ?? 0.0,
        isPrimary: host == _primaryHost,
        isCandidate: _candidateHosts.contains(host),
      );
    }).toList();

    nodes.sort((a, b) {
      if (a.isPrimary && !b.isPrimary) return -1;
      if (!a.isPrimary && b.isPrimary) return 1;
      if (a.isCandidate && !b.isCandidate) return -1;
      if (!a.isCandidate && b.isCandidate) return 1;
      return b.speedBps.compareTo(a.speedBps);
    });

    return BtrSnapshot(
      downloadSpeedBps: currentSpeedBps,
      activeConnections: _activeConnections,
      maxConcurrency: _maxConcurrency,
      rescuedChunks: _rescuedChunks,
      totalBytes: _totalDownloadedBytes,
      nodes: nodes,
      activeThreads: _activeThreads.values.map((t) => t.toStat()).toList(),
      recentThreads: List.unmodifiable(_recentThreads),
      currentPrimaryHost: _primaryHost,
      candidateHosts: _candidateHosts.toList(),
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
    _activeThreads.clear();
    _recentThreads.clear();
    _primaryHost = null;
    _candidateHosts.clear();
    _notify();
  }

  void dispose() {
    _streamController.close();
  }
}
