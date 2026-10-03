package com.github.h3yow.marketdataaggregatorjava.domain;

/**
 * 一个固定时间窗口的 K 线（或缺口标记）。
 * gap=true 时 open/high/low/close 为 null、volume=0。
 *
 * @param corrected 该窗口是否由迟到数据回补修正过
 * @param version   窗口版本，初始为 1，每次回补 +1
 * @param settled   该窗口是否已结算（水位越过 窗口终点+乱序容忍，之后不再可能被修正）
 */
public record Candle(String symbol,
                     long windowStart,
                     long windowEnd,
                     Double open,
                     Double high,
                     Double low,
                     Double close,
                     long volume,
                     boolean gap,
                     boolean corrected,
                     long version,
                     boolean settled) {
}
