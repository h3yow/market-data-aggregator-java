package com.github.h3yow.marketdataaggregatorjava.model;

/**
 * 一个固定时间窗口的K线。
 */
public class Candle {
    private String symbol;
    private long windowStart;
    private long windowEnd;
    private long openPrice;
    private long highPrice;
    private long lowPrice;
    private long closePrice;
    private long volume;
    private long firstTickTime;
    private long lastTickTime;
    private WindowStatus status;
    private long updateSeq;

    public Candle() {
    }

    public String getSymbol() { return symbol; }
    public void setSymbol(String symbol) { this.symbol = symbol; }
    public long getWindowStart() { return windowStart; }
    public void setWindowStart(long windowStart) { this.windowStart = windowStart; }
    public long getWindowEnd() { return windowEnd; }
    public void setWindowEnd(long windowEnd) { this.windowEnd = windowEnd; }
    public long getOpenPrice() { return openPrice; }
    public void setOpenPrice(long openPrice) { this.openPrice = openPrice; }
    public long getHighPrice() { return highPrice; }
    public void setHighPrice(long highPrice) { this.highPrice = highPrice; }
    public long getLowPrice() { return lowPrice; }
    public void setLowPrice(long lowPrice) { this.lowPrice = lowPrice; }
    public long getClosePrice() { return closePrice; }
    public void setClosePrice(long closePrice) { this.closePrice = closePrice; }
    public long getVolume() { return volume; }
    public void setVolume(long volume) { this.volume = volume; }
    public long getFirstTickTime() { return firstTickTime; }
    public void setFirstTickTime(long firstTickTime) { this.firstTickTime = firstTickTime; }
    public long getLastTickTime() { return lastTickTime; }
    public void setLastTickTime(long lastTickTime) { this.lastTickTime = lastTickTime; }
    public WindowStatus getStatus() { return status; }
    public void setStatus(WindowStatus status) { this.status = status; }
    public long getUpdateSeq() { return updateSeq; }
    public void setUpdateSeq(long updateSeq) { this.updateSeq = updateSeq; }
}
