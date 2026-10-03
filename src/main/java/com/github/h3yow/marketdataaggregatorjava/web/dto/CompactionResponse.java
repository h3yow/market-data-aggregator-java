package com.github.h3yow.marketdataaggregatorjava.web.dto;

/**
 * 事件流压缩/归档结果对外视图。
 */
public record CompactionResponse(String symbol,
                                 long archivedToSeq,
                                 int archivedCount,
                                 long hotBaseSeq,
                                 boolean trimmed) {
}
