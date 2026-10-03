package com.github.h3yow.marketdataaggregatorjava.domain;

/**
 * 面向客户端增量拉取的事件类型。
 */
public enum EventType {

    /** 窗口首次产生数据 */
    WINDOW_OPEN,

    /** 当前开放窗口并入新的正序行情，窗口快照被刷新（窗口仍未结算） */
    WINDOW_UPDATE,

    /** 水位推进时发现中间窗口无任何行情，标记为缺口 */
    GAP_MARKED,

    /** 迟到数据回补了此前已标记为缺口的窗口，下发修正版 */
    CORRECTION,

    /**
     * 窗口已结算：水位已越过 窗口终点 + 乱序容忍时长，
     * 该窗口此后不可能再被任何迟到数据修正，客户端可安全落库。
     * 事件携带的即该窗口的最终快照。
     */
    WINDOW_SETTLED
}
