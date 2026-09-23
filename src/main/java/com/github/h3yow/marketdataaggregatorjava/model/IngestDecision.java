package com.github.h3yow.marketdataaggregatorjava.model;

/**
 * 单笔行情的处理结果类型。
 */
public enum IngestDecision {
    ACCEPTED_NEW,
    ACCEPTED_LATE_FILL,
    REJECTED_DUPLICATE,
    REJECTED_STALE,
    REJECTED_INVALID
}
