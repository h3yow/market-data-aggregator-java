package com.github.h3yow.marketdataaggregatorjava.domain;

/**
 * 面向客户端增量拉取的事件类型。
 */
public enum EventType {

    /** 窗口首次产生数据 */
    WINDOW_OPEN,

    /** 窗口仍开放时，新到的正序行情更新其 OHLCV（完整快照，保证事件重放可独立重建视图） */
    WINDOW_UPDATE,

    /** 水位推进时发现中间窗口无任何行情，标记为缺口 */
    GAP_MARKED,

    /** 迟到数据回补了此前已标记为缺口的窗口，下发修正版 */
    CORRECTION,

    /** 窗口已被更晚窗口推进且超过乱序容忍时间：状态最终确定，不会再被修正 */
    SETTLED
}
