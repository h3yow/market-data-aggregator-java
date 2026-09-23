package com.github.h3yow.marketdataaggregatorjava.model;

/**
 * 窗口变更事件（用于增量游标）。每次窗口被新建/修正/标记缺口/回补都会产生一条。
 */
public class CandleChange {
    private long seq;
    private long windowStart;
    private WindowStatus status;
    private Candle candle;

    public CandleChange() {
    }

    public long getSeq() { return seq; }
    public void setSeq(long seq) { this.seq = seq; }
    public long getWindowStart() { return windowStart; }
    public void setWindowStart(long windowStart) { this.windowStart = windowStart; }
    public WindowStatus getStatus() { return status; }
    public void setStatus(WindowStatus status) { this.status = status; }
    public Candle getCandle() { return candle; }
    public void setCandle(Candle candle) { this.candle = candle; }
}
