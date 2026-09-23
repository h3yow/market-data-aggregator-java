package com.github.h3yow.marketdataaggregatorjava.domain;

/**
 * 单次行情接入的处理结果。
 *
 * @param status      判定状态
 * @param reason      判定依据的人类可读说明
 * @param windowStart 被接受时归属窗口的起点（epoch millis），拒绝时为 null
 * @param tick        原始输入行情
 */
public record IngestResult(IngestStatus status, String reason, Long windowStart, TradeTick tick) {
}
