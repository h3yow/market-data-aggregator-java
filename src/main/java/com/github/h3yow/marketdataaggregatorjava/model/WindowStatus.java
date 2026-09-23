package com.github.h3yow.marketdataaggregatorjava.model;

/**
 * 窗口状态：有数据 / 无行情缺口 / 已回补。
 */
public enum WindowStatus {
    FILLED,
    GAP,
    BACKFILLED
}
