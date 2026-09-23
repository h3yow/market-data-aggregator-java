package com.github.h3yow.marketdataaggregatorjava.domain;

/**
 * 逐笔成交行情（本地注入）。
 *
 * @param symbol    标的代码
 * @param tradeId   成交唯一 ID，与 symbol 共同构成幂等键
 * @param timestamp 成交时间（epoch millis）
 * @param price     成交价（须为正数）
 * @param volume    成交量（须为正数）
 */
public record TradeTick(String symbol, long tradeId, long timestamp, double price, long volume) {
}
