class BtrByteRange {
  final int start;
  final int? end;

  const BtrByteRange({required this.start, this.end});

  bool get isOpenEnded => end == null;

  int? get length => end != null ? end! - start + 1 : null;

  @override
  String toString() => 'bytes=$start-${end ?? ""}';
}

class BtrContentRange {
  final int start;
  final int end;
  final int? total;

  const BtrContentRange({
    required this.start,
    required this.end,
    this.total,
  });

  int get length => end - start + 1;

  @override
  String toString() => 'bytes $start-$end/${total ?? "*"}';
}

class BtrChunk {
  final int index;
  final int start;
  final int end;

  const BtrChunk({
    required this.index,
    required this.start,
    required this.end,
  });

  int get length => end - start + 1;

  @override
  String toString() => 'Chunk#$index[$start-$end, len=$length]';
}

abstract final class BtrRangeUtils {
  static final RegExp _mediaSuffixRegex = RegExp(r'\.(?:m4s|mp4|flv)$', caseSensitive: false);
  static final RegExp _mediaHostRegex = RegExp(
    r'(?:^|\.)(?:bilivideo\.(?:com|cn|net)|akamaized\.net|szbdyd\.com|hdslb\.com|xycdn\.com|mountaintoys\.cn|nexusedgeio\.com|ahdohpiechei\.com)$',
    caseSensitive: false,
  );

  static final RegExp _rangeHeaderRegex = RegExp(r'^bytes=(\d+)-(\d+)?$', caseSensitive: false);
  static final RegExp _contentRangeRegex = RegExp(r'^bytes\s+(\d+)-(\d+)\/(\d+|\*)$', caseSensitive: false);

  /// Parse HTTP `Range` request header (e.g. `bytes=0-1048575` or `bytes=1048576-`)
  static BtrByteRange? parseRangeHeader(String? value) {
    if (value == null || value.trim().isEmpty) return null;
    final match = _rangeHeaderRegex.firstMatch(value.trim());
    if (match == null) return null;
    final start = int.tryParse(match.group(1)!);
    if (start == null || start < 0) return null;
    final endStr = match.group(2);
    final end = endStr != null && endStr.isNotEmpty ? int.tryParse(endStr) : null;
    if (end != null && end < start) return null;
    return BtrByteRange(start: start, end: end);
  }

  /// Parse HTTP `Content-Range` response header (e.g. `bytes 0-1048575/52428800`)
  static BtrContentRange? parseContentRange(String? value) {
    if (value == null || value.trim().isEmpty) return null;
    final match = _contentRangeRegex.firstMatch(value.trim());
    if (match == null) return null;
    final start = int.tryParse(match.group(1)!);
    final end = int.tryParse(match.group(2)!);
    if (start == null || end == null || end < start) return null;
    final totalStr = match.group(3);
    final total = (totalStr == null || totalStr == '*') ? null : int.tryParse(totalStr);
    return BtrContentRange(start: start, end: end, total: total);
  }

  /// Split a contiguous byte range [start, end] into balanced chunks
  static List<BtrChunk> splitRange(
    int start,
    int end,
    int concurrency, {
    int minChunkBytes = 128 * 1024,
  }) {
    final length = end - start + 1;
    if (length <= 0) return const [];

    final limit = concurrency.clamp(1, 512);
    final minimum = minChunkBytes.clamp(32 * 1024, 1024 * 1024);
    final count = ((length / minimum).ceil()).clamp(1, limit);
    final base = length ~/ count;
    final remainder = length % count;

    final pieces = <BtrChunk>[];
    var cursor = start;
    for (var index = 0; index < count; index++) {
      final size = base + (index < remainder ? 1 : 0);
      pieces.add(BtrChunk(index: index, start: cursor, end: cursor + size - 1));
      cursor += size;
    }
    return pieces;
  }

  /// Split a contiguous byte range [start, end] into fixed-size streaming chunks
  /// Chunk 0 is optimized to be small (e.g. 128KB) for ultra-low latency playback startup
  static List<BtrChunk> splitIntoStreamingChunks(
    int start,
    int end, {
    int chunkSize = 256 * 1024,
    int firstChunkSize = 128 * 1024,
  }) {
    final length = end - start + 1;
    if (length <= 0) return const [];

    final chunks = <BtrChunk>[];
    var current = start;
    var index = 0;

    while (current <= end) {
      final currentChunkSize = (index == 0 && length > firstChunkSize) ? firstChunkSize : chunkSize;
      final chunkEnd = (current + currentChunkSize - 1).clamp(current, end);
      chunks.add(BtrChunk(index: index++, start: current, end: chunkEnd));
      current = chunkEnd + 1;
    }
    return chunks;
  }

  /// Check whether the URL points to a standard Bilibili video/audio stream
  static bool isBilibiliMediaUrl(String value) {
    try {
      final uri = Uri.parse(value);
      if (!uri.hasScheme || (uri.scheme != 'https' && uri.scheme != 'http')) {
        return false;
      }
      final hostMatch = _mediaHostRegex.hasMatch(uri.host);
      final pathMatch = _mediaSuffixRegex.hasMatch(uri.path) || uri.path.contains('/upgcxcode/');
      return hostMatch && pathMatch;
    } catch (_) {
      return false;
    }
  }

  /// Extract and normalize CDN hostname
  static String normalizeCdnHost(String value) {
    final text = value.trim().toLowerCase();
    if (text.isEmpty || text.length > 253) return '';
    try {
      final uri = Uri.parse(text.contains('://') ? text : 'https://$text');
      final host = uri.host;
      if (_mediaHostRegex.hasMatch(host)) {
        return host;
      }
    } catch (_) {}
    return '';
  }
}
