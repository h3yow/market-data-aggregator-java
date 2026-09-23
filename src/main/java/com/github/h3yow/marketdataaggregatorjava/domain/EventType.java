package com.github.h3yow.marketdataaggregatorjava.domain;

/**
 * 面向客户端增量拉取的事件类型。
 */
public enum EventType {

    /** 窗口首次产生数据 */
    WINDOW_OPEN,

    /** 水位推进时发现中间窗口无任何行情，标记为缺口 */
    GAP_MARKED,

    /** 迟到数据回补了此前已标记为缺口的窗口，下发修正版 */
    CORRECTION
}
