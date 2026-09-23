package com.github.h3yow.marketdataaggregatorjava.model;

import java.util.List;

/**
 * 增量拉取响应。
 */
public class PullResponse {
    private String symbol;
    private String nextCursor;
    private boolean hasMore;
    private List<CandleChange> changes;

    public PullResponse() {
    }

    public String getSymbol() { return symbol; }
    public void setSymbol(String symbol) { this.symbol = symbol; }
    public String getNextCursor() { return nextCursor; }
    public void setNextCursor(String nextCursor) { this.nextCursor = nextCursor; }
    public boolean isHasMore() { return hasMore; }
    public void setHasMore(boolean hasMore) { this.hasMore = hasMore; }
    public List<CandleChange> getChanges() { return changes; }
    public void setChanges(List<CandleChange> changes) { this.changes = changes; }
}
