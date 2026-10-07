package com.github.h3yow.marketdataaggregatorjava.domain;

/**
 * 一次事件压缩的结果报告。
 *
 * <p>压缩只折叠“已结算窗口”的历史事件：每个已结算窗口仅保留其
 * {@link EventType#WINDOW_SETTLED} 事件（携带最终快照），其余事件删除。
 * 所有被保留事件的 seq 不变（不重编号），新事件 seq 继续递增、绝不复用。
 *
 * <p>{@code maxDroppedSeq} 同时是本次压缩后的压缩水位观测值：游标 seq 满足
 * {@code 0 < seq < 水位}（历史多次压缩取最大水位）的旧游标无法再保证逐条一致，
 * 会被拉取接口以 {@code COMPACTED}(410) 明确拒绝并附带恢复起点；seq 不小于水位
 * （或 seq=0 从头拉取）的游标继续可用，从头重放永远能重建全部窗口最终状态。
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
