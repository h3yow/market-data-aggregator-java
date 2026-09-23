package com.github.h3yow.marketdataaggregatorjava.domain;

/**
 * 增量拉取事件流中的一条事件。
 * seq 在同一 symbol 内单调递增、只增不补号，是游标推进的唯一依据。
 *
 * @param seq    事件序列号
 * @param type   事件类型
 * @param candle 对应窗口的 K 线快照（缺口与回补均为完整快照）
 */
public record CursorEvent(long seq, EventType type, Candle candle) {
}
