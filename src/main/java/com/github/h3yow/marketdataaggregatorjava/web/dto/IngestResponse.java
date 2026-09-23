package com.github.h3yow.marketdataaggregatorjava.web.dto;

/**
 * 接入结果对外视图。
 */
public record IngestResponse(String status, String reason, Long windowStart) {
}
