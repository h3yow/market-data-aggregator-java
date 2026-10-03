package com.github.h3yow.marketdataaggregatorjava.domain;

/**
 * 一次事件流压缩/归档的结果（骨架占位，实现见 service 层）。
 *
 * @param symbol           标的
 * @param archivedToSeq    本次移入归档的最大事件 seq（无事件可归档时为 0）
 * @param archivedCount    本次归档的事件条数
 * @param hotBaseSeq       热日志当前起点 seq（热日志保存 seq &gt; hotBaseSeq 的事件；
 *                         归档可回放时 hotBaseSeq=0，物理清除后为已清除的最大 seq）
 * @param trimmed          是否物理清除了归档（true 时早于 hotBaseSeq 的游标将判 UNAVAILABLE）
 */
public record CompactionResult(String symbol,
                               long archivedToSeq,
                               int archivedCount,
                               long hotBaseSeq,
                               boolean trimmed) {
}
