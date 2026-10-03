package com.github.h3yow.marketdataaggregatorjava.web.dto;

/**
 * K 线/缺口对外视图。
 */
public record CandleResponse(long windowStart,
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
