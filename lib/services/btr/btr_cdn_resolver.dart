import 'dart:math';
import 'package:PiliPlus/services/btr/btr_config.dart';
import 'package:PiliPlus/services/btr/btr_range.dart';
import 'package:PiliPlus/services/btr/btr_stats.dart';

class _RouteMetric {
  int lastSuccessAt = 0;
  int lastMeasuredAt = 0;
  double bps = 0.0;
}

class _HealthMetric {
  int failures = 0;
  int blockedUntil = 0;
  int lastSuccessAt = 0;
}

class BtrCdnResolver {
  static const List<String> mainlandHosts = [
    'upos-sz-mirrorali.bilivideo.com',
    'upos-sz-mirrorhw.bilivideo.com',
    'upos-sz-mirrorcos.bilivideo.com',
    'upos-sz-mirror08c.bilivideo.com',
    'upos-sz-mirrorbd.bilivideo.com',
    'upos-sz-mirror14b.bilivideo.com',
    'upos-sz-estgoss.bilivideo.com',
    'upos-sz-mirrorbos.bilivideo.com',
  ];

  static const List<String> overseasHosts = [
    'upos-sz-mirrorcosov.bilivideo.com',
    'upos-sz-mirroraliov.bilivideo.com',
    'cn-hk-eq-01-01.bilivideo.com',
    'cn-hk-eq-01-03.bilivideo.com',
  ];

  static const int measurementTtlMs = 90000;

  final Map<String, _HealthMetric> _health = {};
  final Map<String, _RouteMetric> _routes = {};
  final Map<String, DateTime> _bannedHosts = {};
  final Map<String, int> _emptyReplyStrikes = {};
  final Map<String, double> _hostSpeeds = {};

  int _cursor = 0;
  int _rangeCursor = 0;
  int _mediaRangeCount = 0;

  static bool isAkamaiUrl(String url) {
    try {
      final uri = Uri.parse(url);
      return uri.host.toLowerCase().endsWith('.akamaized.net');
    } catch (_) {
      return false;
    }
  }

  String _routeOf(String url) {
    try {
      final uri = Uri.parse(url);
      return '${uri.host}${uri.path}';
    } catch (_) {
      return url;
    }
  }

  String _hostOf(String url) {
    try {
      return Uri.parse(url).host.toLowerCase();
    } catch (_) {
      return '';
    }
  }

  bool _isMeasured(String url, int now) {
    final metric = _routes[_routeOf(url)];
    if (metric == null || metric.lastMeasuredAt == 0) return false;
    return now - metric.lastMeasuredAt < measurementTtlMs;
  }

  double speed(String url) {
    final now = DateTime.now().millisecondsSinceEpoch;
    return _isMeasured(url, now) ? (_routes[_routeOf(url)]?.bps ?? 0.0) : 0.0;
  }

  bool isHostBanned(String host) {
    final expire = _bannedHosts[host];
    if (expire == null) return false;
    if (DateTime.now().isBefore(expire)) return true;
    _bannedHosts.remove(host);
    return false;
  }

  void banHost(String host, {Duration duration = const Duration(minutes: 15)}) {
    _bannedHosts[host] = DateTime.now().add(duration);
    BtrStats.instance.updateNodeStatus(host, 'banned', _hostSpeeds[host] ?? 0);
  }

  String? swapHost(String rawUrl, String targetHost, {bool allowAkamai = false}) {
    if (!allowAkamai && isAkamaiUrl(rawUrl)) return null;
    final host = targetHost.trim().toLowerCase();
    if (BtrRangeUtils.normalizeCdnHost(host) != host) return null;
    try {
      final uri = Uri.parse(rawUrl);
      return uri.replace(scheme: 'https', host: host).toString();
    } catch (_) {
      return null;
    }
  }

  List<String> resolveCandidateUrls({
    required String primaryUrl,
    required Iterable<String> backupUrls,
    required BtrMode mode,
    List<String> customHosts = const [],
    String? preferredHost,
  }) {
    final originals = <String>[];
    if (BtrRangeUtils.isBilibiliMediaUrl(primaryUrl)) originals.add(primaryUrl);
    for (final u in backupUrls) {
      if (BtrRangeUtils.isBilibiliMediaUrl(u) && !originals.contains(u)) {
        originals.add(u);
      }
    }

    final List<String> targetHosts;
    if (mode == BtrMode.custom && customHosts.isNotEmpty) {
      targetHosts = customHosts.map(BtrRangeUtils.normalizeCdnHost).where((h) => h.isNotEmpty).toList();
    } else if (mode == BtrMode.overseas) {
      targetHosts = overseasHosts;
    } else {
      targetHosts = mainlandHosts;
    }

    final donor = originals.firstWhere(
      (u) => !isAkamaiUrl(u),
      orElse: () => originals.isNotEmpty ? originals.first : primaryUrl,
    );

    final synthetic = <String>[];
    for (final host in targetHosts) {
      if (isHostBanned(host)) continue;
      final swapped = swapHost(donor, host, allowAkamai: true);
      if (swapped != null && !synthetic.contains(swapped)) {
        synthetic.add(swapped);
      }
    }

    final result = <String>[];

    // Priority 1: User-preferred CDN host if matched
    if (preferredHost != null && preferredHost.isNotEmpty) {
      final preferred = synthetic.where((u) => _hostOf(u) == preferredHost.toLowerCase());
      result.addAll(preferred);
    }

    // Priority 2: Synthetic mirror URLs
    for (final u in synthetic) {
      if (!result.contains(u)) result.add(u);
    }

    // Priority 3: Original URLs
    for (final u in originals) {
      if (!isHostBanned(_hostOf(u)) && !result.contains(u)) {
        result.add(u);
      }
    }

    if (result.isEmpty) {
      return [primaryUrl, ...backupUrls];
    }
    return result;
  }

  List<String> rangeCandidates(List<String> allUrls) {
    final now = DateTime.now().millisecondsSinceEpoch;
    final available = allUrls.where((u) {
      final h = _health[u];
      return h == null || h.blockedUntil <= now;
    }).toList();

    final pool = available.isNotEmpty ? available : allUrls;

    pool.sort((a, b) {
      final sa = speed(a);
      final sb = speed(b);
      return sb.compareTo(sa);
    });

    final width = min(pool.length, _mediaRangeCount == 0 ? pool.length : 6);
    if (_mediaRangeCount < 2) {
      _mediaRangeCount++;
      return pool.take(width).toList();
    }

    // Exploration slot for unmeasured nodes
    final measured = pool.where((u) => _isMeasured(u, now)).toList();
    final unmeasured = pool.where((u) => !_isMeasured(u, now)).toList();

    final explorePlaces = min(unmeasured.length, max(1, width - measured.length));
    final explore = <String>[];
    for (var i = 0; i < explorePlaces; i++) {
      explore.add(unmeasured[(_rangeCursor + i) % unmeasured.length]);
    }
    if (unmeasured.isNotEmpty) {
      _rangeCursor = (_rangeCursor + explorePlaces) % unmeasured.length;
    }

    final selected = [...measured.take(width - explore.length), ...explore];
    for (final u in pool) {
      if (selected.length >= min(3, pool.length)) break;
      if (!selected.contains(u)) selected.add(u);
    }

    _mediaRangeCount++;
    return selected;
  }

  List<String> startupCandidates(List<String> allUrls) {
    final now = DateTime.now().millisecondsSinceEpoch;
    final available = allUrls.where((u) {
      final h = _health[u];
      return h == null || h.blockedUntil <= now;
    }).toList();
    final pool = available.isNotEmpty ? available : allUrls;
    return pool.take(min(8, pool.length)).toList();
  }

  List<String> rescueCandidates(List<String> allUrls) {
    final now = DateTime.now().millisecondsSinceEpoch;
    final available = allUrls.where((u) {
      final h = _health[u];
      return h == null || h.blockedUntil <= now;
    }).toList();

    final pool = available.isNotEmpty ? available : allUrls;
    pool.sort((a, b) => speed(b).compareTo(speed(a)));
    return pool;
  }

  List<String> ordered(List<String> allUrls, int pieceIndex, {Set<String>? exclude}) {
    final now = DateTime.now().millisecondsSinceEpoch;
    final candidates = allUrls.where((u) => exclude == null || !exclude.contains(u)).toList();
    final available = candidates.where((u) {
      final h = _health[u];
      return h == null || h.blockedUntil <= now;
    }).toList();
    final pool = available.isNotEmpty ? available : candidates;
    if (pool.isEmpty) return const [];

    final offset = (_cursor + pieceIndex) % pool.length;
    final rotated = [...pool.sublist(offset), ...pool.sublist(0, offset)];
    _cursor = (_cursor + 1) % pool.length;
    return rotated;
  }

  void recordSuccess(String url, double bps) {
    final host = _hostOf(url);
    _emptyReplyStrikes.remove(host);

    final now = DateTime.now().millisecondsSinceEpoch;
    final h = _health.putIfAbsent(url, _HealthMetric.new);
    h.failures = 0;
    h.blockedUntil = 0;
    h.lastSuccessAt = now;

    final routeKey = _routeOf(url);
    final r = _routes.putIfAbsent(routeKey, _RouteMetric.new);
    r.lastSuccessAt = now;
    if (bps > 0) {
      r.lastMeasuredAt = now;
      r.bps = (r.bps > 0) ? (r.bps * 0.65 + bps * 0.35) : bps;
    }

    _hostSpeeds[host] = r.bps;
    BtrStats.instance.updateNodeStatus(host, 'healthy', r.bps);
  }

  void recordSample(String url, double bps) {
    if (bps <= 0) return;
    final now = DateTime.now().millisecondsSinceEpoch;
    final host = _hostOf(url);
    final routeKey = _routeOf(url);
    final r = _routes.putIfAbsent(routeKey, _RouteMetric.new);
    r.lastMeasuredAt = now;
    r.bps = (r.bps > 0) ? (r.bps * 0.65 + bps * 0.35) : bps;
    _hostSpeeds[host] = r.bps;
    BtrStats.instance.updateNodeStatus(host, 'healthy', r.bps);
  }

  void recordFailure(String url, dynamic error, int receivedBytes, {int? httpStatus}) {
    final host = _hostOf(url);
    final is403 = httpStatus == 403;

    if (receivedBytes == 0) {
      final strikes = (_emptyReplyStrikes[host] ?? 0) + 1;
      _emptyReplyStrikes[host] = strikes;
      if (strikes >= 2 || is403) {
        banHost(host, duration: Duration(minutes: is403 ? 15 : 5));
        return;
      }
    }

    final h = _health.putIfAbsent(url, _HealthMetric.new);
    h.failures++;
    final backoffMs = min(60000, 3000 * (1 << min(h.failures, 4)));
    final now = DateTime.now().millisecondsSinceEpoch;
    h.blockedUntil = now + backoffMs;

    BtrStats.instance.updateNodeStatus(host, 'blocked', _hostSpeeds[host] ?? 0);
  }

  void reset() {
    _health.clear();
    _routes.clear();
    _hostSpeeds.clear();
    _emptyReplyStrikes.clear();
    _bannedHosts.clear();
    _cursor = 0;
    _rangeCursor = 0;
    _mediaRangeCount = 0;
  }
}
