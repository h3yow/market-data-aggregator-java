package com.github.h3yow.marketdataaggregatorjava.web.dto;

import java.util.List;

/**
 * 增量拉取分页对外视图。
 */
public record CursorPageResponse(String symbol,
                                 List<EventView> events,
                                 String nextCursor,
                                 boolean hasMore) {

    /**
     * 单条增量事件。
     */
    public record EventView(long seq, String type, CandleResponse candle) {
    }
}
