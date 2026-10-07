package com.github.h3yow.marketdataaggregatorjava.service;

import com.github.h3yow.marketdataaggregatorjava.domain.CompactionReport;
import org.springframework.stereotype.Service;

/**
 * 事件日志压缩入口。
 *
 * <p>压缩只折叠已结算窗口的历史事件且不重编号 seq，并记录压缩前沿：
 * 续拉位置不早于前沿的游标仍逐条一致；更早的游标会被以
 * {@link CursorException.Reason#COMPACTED} 明确拒绝并返回恢复起点（seq=0、
 * 新游标），客户端从头重放即可无损恢复（详见
 * {@link AggregationStore#compact(String)}）。
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
