package com.github.h3yow.marketdataaggregatorjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 聚合服务配置项。
 */
@ConfigurationProperties(prefix = "market-data")
public class AggregatorProperties {

    /** K线窗口大小（毫秒），必须为正。 */
    private long windowMs = 1000L;

    /** 行情允许迟到的最大毫秒数；超过则拒绝（REJECTED_STALE）。 */
    private long maxLateMs = 5000L;

    /** 游标有效期（毫秒）。 */
    private long cursorTtlMs = 86_400_000L;

    /** 游标签名密钥。 */
    private String cursorSecret = "change-me";

    /** 单次增量拉取最多返回的变更条数。 */
    private int pullBatchSize = 500;

    public long getWindowMs() { return windowMs; }
    public void setWindowMs(long windowMs) { this.windowMs = windowMs; }
    public long getMaxLateMs() { return maxLateMs; }
    public void setMaxLateMs(long maxLateMs) { this.maxLateMs = maxLateMs; }
    public long getCursorTtlMs() { return cursorTtlMs; }
    public void setCursorTtlMs(long cursorTtlMs) { this.cursorTtlMs = cursorTtlMs; }
    public String getCursorSecret() { return cursorSecret; }
    public void setCursorSecret(String cursorSecret) { this.cursorSecret = cursorSecret; }
    public int getPullBatchSize() { return pullBatchSize; }
    public void setPullBatchSize(int pullBatchSize) { this.pullBatchSize = pullBatchSize; }
}
