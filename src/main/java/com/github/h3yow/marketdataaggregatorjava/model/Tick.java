package com.github.h3yow.marketdataaggregatorjava.model;

/**
 * 逐笔行情（成交）。
 */
public class Tick {
    private String symbol;
    private String tradeId;
    private long price;
    private long volume;
    private long tradeTime;

    public Tick() {
    }

    public String getSymbol() { return symbol; }
    public void setSymbol(String symbol) { this.symbol = symbol; }
    public String getTradeId() { return tradeId; }
    public void setTradeId(String tradeId) { this.tradeId = tradeId; }
    public long getPrice() { return price; }
    public void setPrice(long price) { this.price = price; }
    public long getVolume() { return volume; }
    public void setVolume(long volume) { this.volume = volume; }
    public long getTradeTime() { return tradeTime; }
    public void setTradeTime(long tradeTime) { this.tradeTime = tradeTime; }
}
