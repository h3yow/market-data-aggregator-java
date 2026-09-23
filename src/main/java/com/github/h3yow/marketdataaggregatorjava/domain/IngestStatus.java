package com.github.h3yow.marketdataaggregatorjava.domain;

/**
 * 逐笔行情接入的判定结果，每种结果互斥且含义可区分。
 */
public enum IngestStatus {

    /** 正常接受：时间戳不早于当前水位 */
    ACCEPTED,

    /** 接受但属于乱序/迟到：时间戳早于水位但仍在乱序容忍范围内，按时间戳归位聚合 */
    ACCEPTED_LATE,

    /** 拒绝：(symbol, tradeId) 已处理过，重复推送 */
    DUPLICATE,

    /** 拒绝：字段非法（空标的、非正价格/数量/时间戳、NaN 等） */
    REJECTED_INVALID,

    /** 拒绝：时间戳倒退过多，超出乱序容忍范围 */
    REJECTED_STALE
}
