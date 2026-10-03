package com.github.h3yow.marketdataaggregatorjava.domain;

/**
 * 一次事件压缩的结果报告。
 *
 * <p>压缩只折叠“已结算窗口”的历史事件：每个已结算窗口仅保留其
 * {@link EventType#WINDOW_SETTLED} 事件（携带最终快照），其余事件删除。
 * 所有被保留事件的 seq 不变（不重编号），因此任何已签发游标在压缩后仍然有效，
 * 继续拉取不会漏掉任何窗口的最终状态，也不会重复收到同一 seq。
 *
 * @param symbol             被压缩的标的
 * @param settledWindows     参与压缩的已结算窗口数
 * @param droppedEvents      被删除的事件数
 * @param keptEvents         压缩后保留的事件数
 * @param maxDroppedSeq      被删除事件中的最大 seq（无删除时为 0），仅用于观测/日志
 */
public record CompactionReport(String symbol,
                               int settledWindows,
                               int droppedEvents,
                               int keptEvents,
                               long maxDroppedSeq) {
}
