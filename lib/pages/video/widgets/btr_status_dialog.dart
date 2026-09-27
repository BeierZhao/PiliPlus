import 'dart:async';
import 'dart:ui';
import 'package:flutter/material.dart';
import 'package:flutter_smart_dialog/flutter_smart_dialog.dart';
import 'package:get/get.dart';
import 'package:PiliPlus/services/btr/btr_service.dart';
import 'package:PiliPlus/services/btr/btr_stats.dart';

class BtrStatusDialog {
  static void show([BuildContext? context]) {
    final ctx = (context != null && context.mounted)
        ? context
        : (Get.overlayContext ?? Get.context);
    if (ctx == null) return;
    showDialog(
      context: ctx,
      barrierColor: Colors.black38,
      barrierDismissible: true,
      barrierLabel: '关闭',
      builder: (dialogContext) => const Dialog(
        backgroundColor: Colors.transparent,
        shadowColor: Colors.transparent,
        surfaceTintColor: Colors.transparent,
        insetPadding: EdgeInsets.symmetric(horizontal: 16, vertical: 24),
        child: BtrStatusFloatingView(),
      ),
    );
  }

  static void dismiss([BuildContext? context]) {
    final ctx = (context != null && context.mounted)
        ? context
        : (Get.overlayContext ?? Get.context);
    if (ctx != null && Navigator.of(ctx, rootNavigator: true).canPop()) {
      Navigator.of(ctx, rootNavigator: true).pop();
    }
  }
}

class BtrStatusFloatingView extends StatefulWidget {
  const BtrStatusFloatingView({super.key});

  @override
  State<BtrStatusFloatingView> createState() => _BtrStatusFloatingViewState();
}

class _BtrStatusFloatingViewState extends State<BtrStatusFloatingView> {
  Offset _dragOffset = Offset.zero;
  bool _isCollapsed = false;
  int _selectedTabIndex = 0; // 0: 线程监控, 1: CDN状态
  Timer? _refreshTimer;
  StreamSubscription<BtrSnapshot>? _sub;

  BtrSnapshot _snapshot = BtrStats.instance.snapshot;

  @override
  void initState() {
    super.initState();
    _sub = BtrStats.instance.stream.listen((snap) {
      if (mounted) {
        setState(() {
          _snapshot = snap;
        });
      }
    });
    // Fallback timer to keep UI fresh even when no active chunk stream event fires
    _refreshTimer = Timer.periodic(const Duration(milliseconds: 500), (_) {
      if (mounted) {
        setState(() {
          _snapshot = BtrStats.instance.snapshot;
        });
      }
    });
  }

  @override
  void dispose() {
    _sub?.cancel();
    _refreshTimer?.cancel();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final colorScheme = theme.colorScheme;
    final isDark = theme.brightness == Brightness.dark;

    final containerColor = isDark
        ? const Color(0xE61E1E26)
        : const Color(0xF2FFFFFF);
    final borderColor = isDark
        ? Colors.white.withValues(alpha: 0.12)
        : Colors.black.withValues(alpha: 0.08);

    final isBtrEnabled = BtrService.instance.isEnabled;

    return Transform.translate(
      offset: _dragOffset,
      child: Material(
        type: MaterialType.transparency,
        child: ClipRRect(
          borderRadius: BorderRadius.circular(16),
          child: BackdropFilter(
            filter: ImageFilter.blur(sigmaX: 12, sigmaY: 12),
            child: Container(
              width: _isCollapsed ? 260 : 360,
              constraints: BoxConstraints(
                maxHeight: _isCollapsed ? 64 : 460,
              ),
              decoration: BoxDecoration(
                color: containerColor,
                borderRadius: BorderRadius.circular(16),
                border: Border.all(color: borderColor),
                boxShadow: [
                  BoxShadow(
                    color: Colors.black.withValues(alpha: 0.25),
                    blurRadius: 20,
                    offset: const Offset(0, 8),
                  ),
                ],
              ),
              child: _isCollapsed
                  ? _buildCollapsedView(colorScheme)
                  : _buildFullView(theme, colorScheme, isBtrEnabled),
            ),
          ),
        ),
      ),
    );
  }

  /// Compact floating pill view
  Widget _buildCollapsedView(ColorScheme colorScheme) {
    return GestureDetector(
      onPanUpdate: (details) {
        setState(() {
          _dragOffset += details.delta;
        });
      },
      child: Container(
        padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
        child: Row(
          children: [
            const Icon(Icons.bolt, color: Colors.amber, size: 20),
            const SizedBox(width: 6),
            Expanded(
              child: GestureDetector(
                behavior: HitTestBehavior.opaque,
                onTap: () => setState(() => _isCollapsed = false),
                child: Column(
                  mainAxisSize: MainAxisSize.min,
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Text(
                      _snapshot.speedFormatted,
                      style: const TextStyle(
                        fontSize: 14,
                        fontWeight: FontWeight.bold,
                      ),
                    ),
                    Text(
                      '${_snapshot.activeConnections}/${_snapshot.maxConcurrency} 线程',
                      style: TextStyle(
                        fontSize: 11,
                        color: colorScheme.outline,
                      ),
                    ),
                  ],
                ),
              ),
            ),
            IconButton(
              tooltip: '展开',
              iconSize: 18,
              padding: EdgeInsets.zero,
              constraints: const BoxConstraints(minWidth: 28, minHeight: 28),
              icon: const Icon(Icons.open_in_full),
              onPressed: () => setState(() => _isCollapsed = false),
            ),
            IconButton(
              tooltip: '关闭',
              iconSize: 18,
              padding: EdgeInsets.zero,
              constraints: const BoxConstraints(minWidth: 28, minHeight: 28),
              icon: const Icon(Icons.close),
              onPressed: () => BtrStatusDialog.dismiss(context),
            ),
          ],
        ),
      ),
    );
  }

  /// Full status dashboard view
  Widget _buildFullView(ThemeData theme, ColorScheme colorScheme, bool isBtrEnabled) {
    return Column(
      mainAxisSize: MainAxisSize.min,
      crossAxisAlignment: CrossAxisAlignment.stretch,
      children: [
        _buildHeader(colorScheme, isBtrEnabled),
        _buildSummaryCards(colorScheme, isBtrEnabled),
        _buildTabBar(colorScheme),
        Expanded(
          child: _selectedTabIndex == 0
              ? _buildThreadsTab(colorScheme)
              : _buildCdnNodesTab(colorScheme),
        ),
        _buildFooter(colorScheme),
      ],
    );
  }

  /// Drag-friendly header bar
  Widget _buildHeader(ColorScheme colorScheme, bool isBtrEnabled) {
    final speed = _snapshot.downloadSpeedBps;
    final String statusText;
    final Color statusColor;

    if (!isBtrEnabled) {
      statusText = '已停用';
      statusColor = Colors.grey;
    } else if (speed > 1024) {
      statusText = '加速中';
      statusColor = Colors.green;
    } else if (_snapshot.activeConnections > 0) {
      statusText = '连接中';
      statusColor = Colors.blue;
    } else {
      statusText = '待命中';
      statusColor = Colors.orange;
    }

    return GestureDetector(
      behavior: HitTestBehavior.opaque,
      onPanUpdate: (details) {
        setState(() {
          _dragOffset += details.delta;
        });
      },
      child: Container(
        padding: const EdgeInsets.fromLTRB(14, 10, 8, 8),
        decoration: BoxDecoration(
          border: Border(
            bottom: BorderSide(
              color: colorScheme.outlineVariant.withValues(alpha: 0.3),
            ),
          ),
        ),
        child: Row(
          children: [
            const Icon(Icons.speed, size: 20, color: Colors.blueAccent),
            const SizedBox(width: 8),
            const Text(
              '多线程加速状态',
              style: TextStyle(fontSize: 14, fontWeight: FontWeight.bold),
            ),
            const SizedBox(width: 8),
            Container(
              padding: const EdgeInsets.symmetric(horizontal: 6, vertical: 2),
              decoration: BoxDecoration(
                color: statusColor.withValues(alpha: 0.15),
                borderRadius: BorderRadius.circular(6),
                border: Border.all(color: statusColor.withValues(alpha: 0.4)),
              ),
              child: Row(
                mainAxisSize: MainAxisSize.min,
                children: [
                  Container(
                    width: 6,
                    height: 6,
                    decoration: BoxDecoration(
                      shape: BoxShape.circle,
                      color: statusColor,
                    ),
                  ),
                  const SizedBox(width: 4),
                  Text(
                    statusText,
                    style: TextStyle(
                      fontSize: 10,
                      fontWeight: FontWeight.w600,
                      color: statusColor,
                    ),
                  ),
                ],
              ),
            ),
            const Spacer(),
            IconButton(
              tooltip: '最小化',
              iconSize: 18,
              padding: EdgeInsets.zero,
              constraints: const BoxConstraints(minWidth: 28, minHeight: 28),
              icon: const Icon(Icons.close_fullscreen),
              onPressed: () => setState(() => _isCollapsed = true),
            ),
            IconButton(
              tooltip: '关闭',
              iconSize: 18,
              padding: EdgeInsets.zero,
              constraints: const BoxConstraints(minWidth: 28, minHeight: 28),
              icon: const Icon(Icons.close),
              onPressed: () => BtrStatusDialog.dismiss(context),
            ),
          ],
        ),
      ),
    );
  }

  /// Top summary cards (Speed, Threads, Current Primary CDN)
  Widget _buildSummaryCards(ColorScheme colorScheme, bool isBtrEnabled) {
    final primaryHost = _snapshot.currentPrimaryHost ?? '智能优选';
    final primaryShort = primaryHost.replaceFirst('.bilivideo.com', '');

    return Container(
      padding: const EdgeInsets.fromLTRB(12, 10, 12, 6),
      child: Column(
        children: [
          Row(
            children: [
              // Speed card
              Expanded(
                child: Container(
                  padding: const EdgeInsets.all(8),
                  decoration: BoxDecoration(
                    color: colorScheme.surfaceContainerHighest.withValues(alpha: 0.35),
                    borderRadius: BorderRadius.circular(10),
                  ),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Text(
                        '下载总速度',
                        style: TextStyle(fontSize: 11, color: colorScheme.outline),
                      ),
                      const SizedBox(height: 2),
                      Text(
                        _snapshot.speedFormatted,
                        style: const TextStyle(
                          fontSize: 16,
                          fontWeight: FontWeight.bold,
                          color: Colors.green,
                        ),
                      ),
                    ],
                  ),
                ),
              ),
              const SizedBox(width: 8),
              // Concurrency card
              Expanded(
                child: Container(
                  padding: const EdgeInsets.all(8),
                  decoration: BoxDecoration(
                    color: colorScheme.surfaceContainerHighest.withValues(alpha: 0.35),
                    borderRadius: BorderRadius.circular(10),
                  ),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Text(
                        '活跃线程数',
                        style: TextStyle(fontSize: 11, color: colorScheme.outline),
                      ),
                      const SizedBox(height: 2),
                      Text(
                        '${_snapshot.activeConnections} / ${_snapshot.maxConcurrency} 线程',
                        style: TextStyle(
                          fontSize: 16,
                          fontWeight: FontWeight.bold,
                          color: colorScheme.primary,
                        ),
                      ),
                    ],
                  ),
                ),
              ),
            ],
          ),
          const SizedBox(height: 6),
          // Current connected primary CDN
          Container(
            width: double.infinity,
            padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 6),
            decoration: BoxDecoration(
              color: colorScheme.surfaceContainerHighest.withValues(alpha: 0.25),
              borderRadius: BorderRadius.circular(8),
            ),
            child: Row(
              children: [
                const Icon(Icons.cloud_outlined, size: 14, color: Colors.blueAccent),
                const SizedBox(width: 6),
                const Text(
                  '当前连接CDN: ',
                  style: TextStyle(fontSize: 11, fontWeight: FontWeight.w500),
                ),
                Expanded(
                  child: Text(
                    primaryShort,
                    style: TextStyle(
                      fontSize: 11,
                      fontWeight: FontWeight.w600,
                      color: colorScheme.primary,
                    ),
                    maxLines: 1,
                    overflow: TextOverflow.ellipsis,
                  ),
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }

  /// Segmented switch between Threads and CDN Nodes
  Widget _buildTabBar(ColorScheme colorScheme) {
    return Container(
      margin: const EdgeInsets.fromLTRB(12, 6, 12, 6),
      decoration: BoxDecoration(
        color: colorScheme.surfaceContainerHighest.withValues(alpha: 0.4),
        borderRadius: BorderRadius.circular(8),
      ),
      child: Row(
        children: [
          Expanded(
            child: _buildTabButton(
              title: '线程监控 (${_snapshot.activeThreads.length})',
              index: 0,
              colorScheme: colorScheme,
            ),
          ),
          Expanded(
            child: _buildTabButton(
              title: 'CDN状态 (${_snapshot.nodes.length})',
              index: 1,
              colorScheme: colorScheme,
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildTabButton({
    required String title,
    required int index,
    required ColorScheme colorScheme,
  }) {
    final isSelected = _selectedTabIndex == index;
    return GestureDetector(
      behavior: HitTestBehavior.opaque,
      onTap: () => setState(() => _selectedTabIndex = index),
      child: Container(
        padding: const EdgeInsets.symmetric(vertical: 6),
        decoration: BoxDecoration(
          color: isSelected ? colorScheme.primary : Colors.transparent,
          borderRadius: BorderRadius.circular(8),
        ),
        alignment: Alignment.center,
        child: Text(
          title,
          style: TextStyle(
            fontSize: 12,
            fontWeight: isSelected ? FontWeight.bold : FontWeight.normal,
            color: isSelected ? colorScheme.onPrimary : colorScheme.onSurface,
          ),
        ),
      ),
    );
  }

  /// Threads monitor tab (showing each thread's download speed, host, and chunk progress)
  Widget _buildThreadsTab(ColorScheme colorScheme) {
    final active = _snapshot.activeThreads;
    final recent = _snapshot.recentThreads;

    if (active.isEmpty && recent.isEmpty) {
      return Center(
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            Icon(Icons.pause_circle_outline, size: 32, color: colorScheme.outline),
            const SizedBox(height: 6),
            Text(
              '当前缓冲已充盈，线程待命中',
              style: TextStyle(fontSize: 12, color: colorScheme.outline),
            ),
          ],
        ),
      );
    }

    return ListView(
      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 4),
      children: [
        if (active.isNotEmpty) ...[
          Padding(
            padding: const EdgeInsets.only(bottom: 6),
            child: Text(
              '活跃拉取线程 (${active.length}):',
              style: TextStyle(fontSize: 11, color: colorScheme.outline, fontWeight: FontWeight.bold),
            ),
          ),
          ...active.map((t) => _buildThreadItem(t, colorScheme, isActive: true)),
        ],
        if (active.isEmpty && recent.isNotEmpty) ...[
          Container(
            padding: const EdgeInsets.symmetric(vertical: 6, horizontal: 8),
            margin: const EdgeInsets.only(bottom: 8),
            decoration: BoxDecoration(
              color: Colors.amber.withValues(alpha: 0.1),
              borderRadius: BorderRadius.circular(6),
            ),
            child: const Row(
              children: [
                Icon(Icons.check_circle_outline, size: 14, color: Colors.amber),
                SizedBox(width: 6),
                Text(
                  '播放器缓冲充裕，显示最近分片完成速度:',
                  style: TextStyle(fontSize: 11, color: Colors.amber, fontWeight: FontWeight.w500),
                ),
              ],
            ),
          ),
          ...recent.take(6).map((t) => _buildThreadItem(t, colorScheme, isActive: false)),
        ],
      ],
    );
  }

  Widget _buildThreadItem(BtrThreadStat thread, ColorScheme colorScheme, {required bool isActive}) {
    final hostShort = thread.host.replaceFirst('.bilivideo.com', '');
    return Container(
      margin: const EdgeInsets.only(bottom: 6),
      padding: const EdgeInsets.all(8),
      decoration: BoxDecoration(
        color: colorScheme.surfaceContainerHighest.withValues(alpha: 0.3),
        borderRadius: BorderRadius.circular(8),
        border: Border.all(
          color: isActive
              ? Colors.green.withValues(alpha: 0.3)
              : colorScheme.outlineVariant.withValues(alpha: 0.2),
        ),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Container(
                padding: const EdgeInsets.symmetric(horizontal: 4, vertical: 1),
                decoration: BoxDecoration(
                  color: thread.isAudio
                      ? Colors.orange.withValues(alpha: 0.2)
                      : Colors.blue.withValues(alpha: 0.2),
                  borderRadius: BorderRadius.circular(4),
                ),
                child: Text(
                  thread.isAudio ? '音频' : '视频',
                  style: TextStyle(
                    fontSize: 9,
                    fontWeight: FontWeight.bold,
                    color: thread.isAudio ? Colors.orange : Colors.blue,
                  ),
                ),
              ),
              const SizedBox(width: 6),
              Text(
                '线程 #${thread.threadId}',
                style: const TextStyle(fontSize: 11, fontWeight: FontWeight.w600),
              ),
              const SizedBox(width: 6),
              Expanded(
                child: Text(
                  hostShort,
                  style: TextStyle(fontSize: 10, color: colorScheme.outline),
                  overflow: TextOverflow.ellipsis,
                ),
              ),
              Text(
                thread.speedFormatted,
                style: TextStyle(
                  fontSize: 12,
                  fontWeight: FontWeight.bold,
                  color: isActive ? Colors.green : colorScheme.onSurface,
                ),
              ),
            ],
          ),
          const SizedBox(height: 4),
          Row(
            children: [
              Text(
                '分片 #${thread.chunkIndex} (${thread.receivedFormatted} / ${thread.totalFormatted})',
                style: TextStyle(fontSize: 10, color: colorScheme.outline),
              ),
              const Spacer(),
              Text(
                '${(thread.progress * 100).toStringAsFixed(0)}%',
                style: TextStyle(fontSize: 10, color: colorScheme.outline),
              ),
            ],
          ),
          const SizedBox(height: 3),
          ClipRRect(
            borderRadius: BorderRadius.circular(2),
            child: LinearProgressIndicator(
              value: isActive ? (thread.progress > 0 ? thread.progress : null) : 1.0,
              minHeight: 3,
              backgroundColor: colorScheme.surfaceContainerHighest,
              color: isActive ? Colors.green : Colors.grey,
            ),
          ),
        ],
      ),
    );
  }

  /// CDN Nodes and health status tab
  Widget _buildCdnNodesTab(ColorScheme colorScheme) {
    final nodes = _snapshot.nodes;

    if (nodes.isEmpty) {
      return Center(
        child: Text(
          '尚未解析到 CDN 节点',
          style: TextStyle(fontSize: 12, color: colorScheme.outline),
        ),
      );
    }

    return ListView.builder(
      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 4),
      itemCount: nodes.length,
      itemBuilder: (context, index) {
        final node = nodes[index];
        final hostShort = node.host.replaceFirst('.bilivideo.com', '');

        final Color statusColor;
        final String statusLabel;

        switch (node.state) {
          case 'healthy':
            statusColor = Colors.green;
            statusLabel = '正常';
            break;
          case 'blocked':
            statusColor = Colors.orange;
            statusLabel = '受限';
            break;
          case 'banned':
            statusColor = Colors.red;
            statusLabel = '熔断';
            break;
          default:
            statusColor = Colors.grey;
            statusLabel = '待测';
        }

        return Container(
          margin: const EdgeInsets.only(bottom: 6),
          padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 8),
          decoration: BoxDecoration(
            color: colorScheme.surfaceContainerHighest.withValues(alpha: 0.3),
            borderRadius: BorderRadius.circular(8),
            border: node.isPrimary
                ? Border.all(color: Colors.blueAccent.withValues(alpha: 0.4))
                : null,
          ),
          child: Row(
            children: [
              Container(
                width: 7,
                height: 7,
                decoration: BoxDecoration(
                  shape: BoxShape.circle,
                  color: statusColor,
                ),
              ),
              const SizedBox(width: 8),
              Expanded(
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Row(
                      children: [
                        Flexible(
                          child: Text(
                            hostShort,
                            style: const TextStyle(
                              fontSize: 11,
                              fontWeight: FontWeight.w600,
                            ),
                            overflow: TextOverflow.ellipsis,
                          ),
                        ),
                        if (node.isPrimary) ...[
                          const SizedBox(width: 4),
                          Container(
                            padding: const EdgeInsets.symmetric(horizontal: 4, vertical: 1),
                            decoration: BoxDecoration(
                              color: Colors.blue.withValues(alpha: 0.15),
                              borderRadius: BorderRadius.circular(4),
                            ),
                            child: const Text(
                              '当前主选',
                              style: TextStyle(
                                fontSize: 9,
                                color: Colors.blue,
                                fontWeight: FontWeight.bold,
                              ),
                            ),
                          ),
                        ],
                      ],
                    ),
                    Text(
                      node.host,
                      style: TextStyle(fontSize: 9, color: colorScheme.outline),
                      overflow: TextOverflow.ellipsis,
                    ),
                  ],
                ),
              ),
              const SizedBox(width: 8),
              Column(
                crossAxisAlignment: CrossAxisAlignment.end,
                children: [
                  Text(
                    node.speedFormatted,
                    style: TextStyle(
                      fontSize: 11,
                      fontWeight: FontWeight.bold,
                      color: node.speedBps > 0 ? Colors.green : colorScheme.outline,
                    ),
                  ),
                  Text(
                    statusLabel,
                    style: TextStyle(
                      fontSize: 10,
                      fontWeight: FontWeight.w500,
                      color: statusColor,
                    ),
                  ),
                ],
              ),
            ],
          ),
        );
      },
    );
  }

  /// Bottom footer
  Widget _buildFooter(ColorScheme colorScheme) {
    final rescued = _snapshot.rescuedChunks;
    final totalMB = (_snapshot.totalBytes / (1024 * 1024)).toStringAsFixed(1);

    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 6),
      decoration: BoxDecoration(
        border: Border(
          top: BorderSide(
            color: colorScheme.outlineVariant.withValues(alpha: 0.2),
          ),
        ),
      ),
      child: Row(
        children: [
          Icon(Icons.shield_outlined, size: 12, color: colorScheme.outline),
          const SizedBox(width: 4),
          Text(
            '对冲救援: $rescued 次 · 已加速传输: $totalMB MB',
            style: TextStyle(fontSize: 10, color: colorScheme.outline),
          ),
        ],
      ),
    );
  }
}
