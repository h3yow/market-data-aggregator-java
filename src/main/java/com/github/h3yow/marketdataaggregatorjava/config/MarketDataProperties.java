package com.github.h3yow.marketdataaggregatorjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 行情聚合配置项（前缀 market-data）。
 */
@ConfigurationProperties(prefix = "market-data")
public class MarketDataProperties {

    /** K 线窗口大小（毫秒），默认 1000 */
    private long windowSizeMillis = 1000L;

    /** 乱序/迟到容忍时长（毫秒）：早于水位超过该值的行情被拒绝 */
    private long outOfOrderToleranceMillis = 5000L;

    /** 游标有效期（毫秒） */
    private long cursorTtlMillis = 3600000L;

    /** 游标签名密钥 */
    private String cursorSecret = "change-me-local-secret";

    /** 启动时是否注入确定性本地模拟逐笔行情 */
    private boolean injectorEnabled = true;

    public long getWindowSizeMillis() {
        return windowSizeMillis;
    }

    public void setWindowSizeMillis(long windowSizeMillis) {
        this.windowSizeMillis = windowSizeMillis;
    }

    public long getOutOfOrderToleranceMillis() {
        return outOfOrderToleranceMillis;
    }

    public void setOutOfOrderToleranceMillis(long outOfOrderToleranceMillis) {
        this.outOfOrderToleranceMillis = outOfOrderToleranceMillis;
    }

    public long getCursorTtlMillis() {
        return cursorTtlMillis;
    }

    public void setCursorTtlMillis(long cursorTtlMillis) {
        this.cursorTtlMillis = cursorTtlMillis;
    }

    public String getCursorSecret() {
        return cursorSecret;
    }

    public void setCursorSecret(String cursorSecret) {
        this.cursorSecret = cursorSecret;
    }

    public boolean isInjectorEnabled() {
        return injectorEnabled;
    }

    public void setInjectorEnabled(boolean injectorEnabled) {
        this.injectorEnabled = injectorEnabled;
    }
}
