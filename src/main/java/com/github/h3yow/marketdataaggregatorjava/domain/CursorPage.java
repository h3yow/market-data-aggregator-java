package com.github.h3yow.marketdataaggregatorjava.domain;

import java.util.List;

/**
 * 基于游标的增量拉取结果页。
 *
 * @param symbol   标的
 * @param events   seq 严格大于入参游标的事件
 * @param nextCursor 下一页游标（透传可重复拉取，结果不重不漏）
 * @param hasMore  后续是否还有事件
 */
public record CursorPage(String symbol, List<CursorEvent> events, String nextCursor, boolean hasMore) {
}
