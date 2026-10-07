package com.github.h3yow.marketdataaggregatorjava.service;

import com.github.h3yow.marketdataaggregatorjava.domain.CompactionReport;
import org.springframework.stereotype.Service;

/**
 * 事件日志压缩入口。
 *
 * <p>压缩只折叠已结算窗口的历史事件且不重编号 seq。游标兼容范围：
 * seq 已越过压缩水位的游标继续逐条一致；落在压缩水位之前（含）的游标会在下次拉取时
 * 被以 {@code COMPACTED}(410) 明确拒绝并返回恢复起点与新游标；seq=0 从头拉取永远可用
 * （详见 {@link AggregationStore#compact(String)}）。
 * 压缩是显式运维动作：由管理端点或定时任务触发，不在接入路径上自动执行，
 * 保证行为可预期、可复现。
 */
@Service
public class CompactionService {

    private final AggregationStore store;

    public CompactionService(AggregationStore store) {
        this.store = store;
    }

    public CompactionReport compact(String symbol) {
        return store.compact(symbol);
    }
}
