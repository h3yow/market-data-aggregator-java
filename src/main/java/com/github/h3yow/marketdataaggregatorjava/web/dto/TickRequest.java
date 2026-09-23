package com.github.h3yow.marketdataaggregatorjava.web.dto;

/**
 * 行情接入请求体。
 */
public record TickRequest(String symbol, Long tradeId, Long timestamp, Double price, Long volume) {
}
