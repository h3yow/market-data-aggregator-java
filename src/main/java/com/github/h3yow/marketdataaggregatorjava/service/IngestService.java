package com.github.h3yow.marketdataaggregatorjava.service;

import com.github.h3yow.marketdataaggregatorjava.domain.IngestResult;
import com.github.h3yow.marketdataaggregatorjava.domain.IngestStatus;
import com.github.h3yow.marketdataaggregatorjava.domain.TradeTick;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 逐笔行情接入门面：先做字段合法性校验，再委托 {@link AggregationStore}
 * 完成幂等、乱序/迟到与聚合判定。
 *
 * <p>非法字段一律以 {@link IngestStatus#REJECTED_INVALID} 拒绝，
 * 且不进入存储层（不会建立标的状态、不占用幂等索引）。
 */
@Service
public class IngestService {

    private static final Logger log = LoggerFactory.getLogger(IngestService.class);

    private final AggregationStore store;

    public IngestService(AggregationStore store) {
        this.store = store;
    }

    public IngestResult ingest(TradeTick tick) {
        if (tick == null) {
            return invalid(null, "请求体为空");
        }
        if (tick.symbol() == null || tick.symbol().isBlank()) {
            return invalid(tick, "symbol 为空");
        }
        if (tick.tradeId() <= 0) {
            return invalid(tick, "tradeId 必须为正数，实际=" + tick.tradeId());
        }
        if (tick.timestamp() <= 0) {
            return invalid(tick, "timestamp 必须为正数(epoch millis)，实际=" + tick.timestamp());
        }
        if (Double.isNaN(tick.price()) || Double.isInfinite(tick.price()) || tick.price() <= 0) {
            return invalid(tick, "price 必须为正的有限数，实际=" + tick.price());
        }
        if (tick.volume() <= 0) {
            return invalid(tick, "volume 必须为正数，实际=" + tick.volume());
        }
        return store.accept(tick);
    }

    private IngestResult invalid(TradeTick tick, String reason) {
        log.info("[INGEST] tick={} -> REJECTED_INVALID | 判定依据: {}", tick, reason);
        return new IngestResult(IngestStatus.REJECTED_INVALID, reason, null, tick);
    }
}
