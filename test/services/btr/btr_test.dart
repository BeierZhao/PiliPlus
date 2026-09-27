import 'dart:convert';
import 'dart:io';

import 'package:PiliPlus/services/btr/btr_cdn_resolver.dart';
import 'package:PiliPlus/services/btr/btr_config.dart';
import 'package:PiliPlus/services/btr/btr_proxy_server.dart';
import 'package:PiliPlus/services/btr/btr_range.dart';
import 'package:PiliPlus/services/btr/btr_stats.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('BtrRangeUtils Tests', () {
    test('parseRangeHeader parses valid closed range', () {
      final range = BtrRangeUtils.parseRangeHeader('bytes=0-1048575');
      expect(range, isNotNull);
      expect(range!.start, 0);
      expect(range.end, 1048575);
      expect(range.length, 1048576);
      expect(range.isOpenEnded, isFalse);
    });

    test('parseRangeHeader parses open-ended range', () {
      final range = BtrRangeUtils.parseRangeHeader('bytes=1048576-');
      expect(range, isNotNull);
      expect(range!.start, 1048576);
      expect(range.end, isNull);
      expect(range.length, isNull);
      expect(range.isOpenEnded, isTrue);
    });

    test('parseRangeHeader rejects invalid formats', () {
      expect(BtrRangeUtils.parseRangeHeader(null), isNull);
      expect(BtrRangeUtils.parseRangeHeader(''), isNull);
      expect(BtrRangeUtils.parseRangeHeader('invalid'), isNull);
      expect(BtrRangeUtils.parseRangeHeader('bytes=100-50'), isNull);
    });

    test('parseContentRange parses valid content range', () {
      final cr = BtrRangeUtils.parseContentRange('bytes 0-1023/102400');
      expect(cr, isNotNull);
      expect(cr!.start, 0);
      expect(cr.end, 1023);
      expect(cr.total, 102400);
      expect(cr.length, 1024);

      final crAsterisk = BtrRangeUtils.parseContentRange('bytes 0-1023/*');
      expect(crAsterisk, isNotNull);
      expect(crAsterisk!.total, isNull);
    });

    test('splitRange correctly partitions bytes without gap or overlap', () {
      const start = 0;
      const end = 1024 * 1024 - 1; // 1MB
      final pieces = BtrRangeUtils.splitRange(start, end, 8, minChunkBytes: 64 * 1024);

      expect(pieces.length, 8);
      expect(pieces.first.start, 0);
      expect(pieces.last.end, end);

      var totalBytes = 0;
      for (var i = 0; i < pieces.length; i++) {
        expect(pieces[i].index, i);
        totalBytes += pieces[i].length;
        if (i > 0) {
          expect(pieces[i].start, pieces[i - 1].end + 1);
        }
      }
      expect(totalBytes, end - start + 1);
    });

    test('splitRange respects minChunkBytes', () {
      const start = 0;
      const end = 100 * 1024 - 1; // 100KB
      // Requested 8 threads, but minChunkBytes is 64KB -> should split into at most 2 pieces
      final pieces = BtrRangeUtils.splitRange(start, end, 8, minChunkBytes: 64 * 1024);
      expect(pieces.length, lessThanOrEqualTo(2));
      expect(pieces.first.start, 0);
      expect(pieces.last.end, end);
    });

    test('isBilibiliMediaUrl identifies media URLs', () {
      expect(
        BtrRangeUtils.isBilibiliMediaUrl(
          'https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/12/34/56.m4s?e=123',
        ),
        isTrue,
      );
      expect(
        BtrRangeUtils.isBilibiliMediaUrl(
          'https://upos-sz-mirrorali.bilivideo.com/upgcxcode/12/34/56.mp4',
        ),
        isTrue,
      );
      expect(
        BtrRangeUtils.isBilibiliMediaUrl('https://api.bilibili.com/x/web-interface/view'),
        isFalse,
      );
      expect(BtrRangeUtils.isBilibiliMediaUrl('not-a-url'), isFalse);
    });
  });

  group('BtrCdnResolver Tests', () {
    late BtrCdnResolver resolver;

    setUp(() {
      resolver = BtrCdnResolver();
    });

    test('resolves candidates with mainland hosts by default', () {
      const primary = 'https://upos-sz-mirrorali.bilivideo.com/upgcxcode/1/2/3.m4s?token=abc';
      final candidates = resolver.resolveCandidateUrls(
        primaryUrl: primary,
        backupUrls: const [],
        mode: BtrMode.mainland,
      );

      expect(candidates.isNotEmpty, isTrue);
      final hosts = candidates.map((u) => Uri.parse(u).host).toSet();
      expect(hosts.contains('upos-sz-mirrorhw.bilivideo.com'), isTrue);
      expect(hosts.contains('upos-sz-mirrorcos.bilivideo.com'), isTrue);
    });

    test('prioritizes preferred host at the top', () {
      const primary = 'https://upos-sz-mirrorali.bilivideo.com/upgcxcode/1/2/3.m4s?token=abc';
      final candidates = resolver.resolveCandidateUrls(
        primaryUrl: primary,
        backupUrls: const [],
        mode: BtrMode.mainland,
        preferredHost: 'upos-sz-mirrorcos.bilivideo.com',
      );

      expect(candidates.isNotEmpty, isTrue);
      expect(Uri.parse(candidates.first).host, 'upos-sz-mirrorcos.bilivideo.com');
    });

    test('bans failing host after repeated empty reply failures', () {
      const failingHost = 'upos-sz-mirror08c.bilivideo.com';
      const failingUrl = 'https://$failingHost/upgcxcode/1/2/3.m4s';

      expect(resolver.isHostBanned(failingHost), isFalse);

      // Strike 1
      resolver.recordFailure(failingUrl, 'Connection error', 0);
      expect(resolver.isHostBanned(failingHost), isFalse);

      // Strike 2 -> banned
      resolver.recordFailure(failingUrl, 'Connection error', 0);
      expect(resolver.isHostBanned(failingHost), isTrue);

      // Candidates should now exclude banned host
      final candidates = resolver.resolveCandidateUrls(
        primaryUrl: 'https://upos-sz-mirrorali.bilivideo.com/upgcxcode/1/2/3.m4s',
        backupUrls: const [],
        mode: BtrMode.mainland,
      );
      final hosts = candidates.map((u) => Uri.parse(u).host).toSet();
      expect(hosts.contains(failingHost), isFalse);
    });

    test('HTTP 403 immediately bans the host for 15 minutes', () {
      const forbiddenHost = 'upos-sz-mirrorhw.bilivideo.com';
      const forbiddenUrl = 'https://$forbiddenHost/upgcxcode/1/2/3.m4s';

      resolver.recordFailure(forbiddenUrl, 'Forbidden', 0, httpStatus: 403);
      expect(resolver.isHostBanned(forbiddenHost), isTrue);
    });

    test('recordSuccess updates speed and health', () {
      const url = 'https://upos-sz-mirrorali.bilivideo.com/upgcxcode/1/2/3.m4s';
      resolver.recordSuccess(url, 5000000.0); // 5 MB/s
      expect(resolver.speed(url), greaterThan(0));
    });
  });

  group('BtrStats Tests', () {
    test('records bytes and calculates speed', () {
      final stats = BtrStats.instance;
      stats.reset();

      stats.onBytesReceived(1024 * 1024); // 1MB
      expect(stats.totalDownloadedBytes, 1024 * 1024);

      stats.onHedgeRescue();
      expect(stats.rescuedChunks, 1);

      final snap = stats.snapshot;
      expect(snap.rescuedChunks, 1);
      expect(snap.totalBytes, 1024 * 1024);
    });
  });

  group('BtrProxyServer Tests', () {
    late BtrProxyServer server;

    setUp(() async {
      server = BtrProxyServer();
      await server.start();
    });

    tearDown(() async {
      await server.stop();
    });

    test('binds to an ephemeral port and responds to /stats', () async {
      expect(server.isRunning, isTrue);
      expect(server.port, isNotNull);
      expect(server.port!, greaterThan(0));

      final client = HttpClient();
      final req = await client.getUrl(Uri.parse('http://127.0.0.1:${server.port}/stats'));
      final resp = await req.close();
      expect(resp.statusCode, HttpStatus.ok);

      final body = await resp.transform(utf8.decoder).join();
      final json = jsonDecode(body) as Map<String, dynamic>;
      expect(json.containsKey('downloadSpeedBps'), isTrue);
      expect(json.containsKey('rescuedChunks'), isTrue);

      client.close();
    });

    test('returns 404 for unknown session', () async {
      final client = HttpClient();
      final req = await client.getUrl(Uri.parse('http://127.0.0.1:${server.port}/stream?id=non_existent'));
      final resp = await req.close();
      expect(resp.statusCode, HttpStatus.notFound);
      client.close();
    });
  });
}
