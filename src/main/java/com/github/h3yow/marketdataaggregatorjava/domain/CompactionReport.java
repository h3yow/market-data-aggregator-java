package com.github.h3yow.marketdataaggregatorjava.domain;

/**
 * 一次事件压缩的结果报告。
 *
 * <p>压缩只折叠“已结算窗口”的历史事件：每个已结算窗口仅保留其
 * {@link EventType#WINDOW_SETTLED} 事件（携带最终快照），其余事件删除。
 * 所有被保留事件的 seq 不变（不重编号）。
 *
 * <p>游标兼容边界由 {@link #compactedThroughSeq()} 表达：
 * <ul>
 *   <li>游标 seq &gt;= compactedThroughSeq：其位置之后的事件全部保留，
 *       续拉序列与未压缩时逐条一致（不重不漏）；</li>
 *   <li>游标 seq &lt; compactedThroughSeq：续拉位置已被压缩删除，
 *       无法再保证逐条一致，拉取时以
 *       {@link com.github.h3yow.marketdataaggregatorjava.service.CursorException.Reason#COMPACTED}
 *       明确拒绝并返回可恢复起点与新游标。</li>
 * </ul>
 *
 * @param symbol              被压缩的标的
 * @param settledWindows      参与压缩的已结算窗口数
 * @param droppedEvents       被删除的事件数
 * @param keptEvents          压缩后保留的事件数
 * @param maxDroppedSeq       本次被删除事件中的最大 seq（无删除时为 0），仅用于观测/日志
 * @param compactedThroughSeq 压缩前沿：历次压缩被删除事件 seq 的高水位（单调不减），
 *                           是游标“逐条一致 / 不可用”的分界点
 */
public record CompactionReport(String symbol,
                               int settledWindows,
                               int droppedEvents,
                               int keptEvents,
                               long maxDroppedSeq,
                               long compactedThroughSeq) {
}
