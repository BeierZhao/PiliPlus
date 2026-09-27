enum BtrMode {
  mainland('大陆主干优化'),
  overseas('海外直连'),
  custom('自定义节点');

  final String label;
  const BtrMode(this.label);

  static BtrMode fromString(String? value) {
    return switch (value) {
      'overseas' => BtrMode.overseas,
      'custom' => BtrMode.custom,
      _ => BtrMode.mainland,
    };
  }
}

class BtrConfig {
  final bool enabled;
  final BtrMode mode;
  /// Concurrency thread count. 0 means automatic concurrency (8..32 dynamic ladder).
  final int concurrency;
  final List<String> customHosts;
  final int minChunkBytes;
  final int firstByteTimeoutMs;
  final int stallTimeoutMs;
  final int attemptTimeoutMs;
  final int hedgeDelayMs;
  final int maxBufferBytes;

  const BtrConfig({
    this.enabled = false,
    this.mode = BtrMode.mainland,
    this.concurrency = 8,
    this.customHosts = const [],
    this.minChunkBytes = 64 * 1024,
    this.firstByteTimeoutMs = 3500,
    this.stallTimeoutMs = 3500,
    this.attemptTimeoutMs = 12000,
    this.hedgeDelayMs = 600,
    this.maxBufferBytes = 32 * 1024 * 1024,
  });

  bool get isAutoConcurrency => concurrency == 0;

  int get effectiveConcurrency => isAutoConcurrency ? 8 : concurrency.clamp(1, 64);

  BtrConfig copyWith({
    bool? enabled,
    BtrMode? mode,
    int? concurrency,
    List<String>? customHosts,
    int? minChunkBytes,
    int? firstByteTimeoutMs,
    int? stallTimeoutMs,
    int? attemptTimeoutMs,
    int? hedgeDelayMs,
    int? maxBufferBytes,
  }) {
    return BtrConfig(
      enabled: enabled ?? this.enabled,
      mode: mode ?? this.mode,
      concurrency: concurrency ?? this.concurrency,
      customHosts: customHosts ?? this.customHosts,
      minChunkBytes: minChunkBytes ?? this.minChunkBytes,
      firstByteTimeoutMs: firstByteTimeoutMs ?? this.firstByteTimeoutMs,
      stallTimeoutMs: stallTimeoutMs ?? this.stallTimeoutMs,
      attemptTimeoutMs: attemptTimeoutMs ?? this.attemptTimeoutMs,
      hedgeDelayMs: hedgeDelayMs ?? this.hedgeDelayMs,
      maxBufferBytes: maxBufferBytes ?? this.maxBufferBytes,
    );
  }
}
