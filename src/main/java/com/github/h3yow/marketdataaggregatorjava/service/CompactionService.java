package com.github.h3yow.marketdataaggregatorjava.service;

import com.github.h3yow.marketdataaggregatorjava.domain.CompactionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 事件流压缩/归档服务。
 *
 * <p>已结算窗口的事件允许从热事件日志移入归档；只要归档仍可回放，
 * 持有早期游标的客户端继续拉取的结果与压缩前完全一致（不重不漏）。
 * 归档被物理清除后，更早的游标将以 {@link CursorException.Reason#UNAVAILABLE}
 * 明确拒绝，并给出可恢复起点与新游标。
 *
 * <p>事件序号在压缩前后保持全局不变：归档段 + 热段仍是同一个只追加序列，
 * 压缩不删除、不复制、不复用 seq。
 */
@Service
public class CompactionService {

    private static final Logger log = LoggerFactory.getLogger(CompactionService.class);

    private final AggregationStore store;

    public CompactionService(AggregationStore store) {
        this.store = store;
    }

    /**
     * 把全部“所属窗口均已结算”的最长事件前缀移入归档（归档保留、可回放）。
     * 可重复调用；没有新的已结算前缀时结果为空操作，保持幂等。
     */
    public CompactionResult archiveSettled(String symbol) {
        long[] moved = store.archiveSettledPrefix(symbol);
        long archivedToSeq = moved[0];
        int count = (int) moved[1];
        // 归档保留时热日志逻辑起点仍是 0（旧游标经归档仍可完整回放）
        CompactionResult result = new CompactionResult(symbol, archivedToSeq, count, 0L, false);
        log.info("[COMPACT] symbol={} 归档已结算事件 {} 条至 seq={}（归档保留，旧游标仍可回放）",
                symbol, count, archivedToSeq);
        return result;
    }

    /**
     * 归档后物理清除已归档事件；早于新热起点的游标下次拉取将判 UNAVAILABLE。
     */
    public CompactionResult trimArchived(String symbol) {
        long[] moved = store.archiveSettledPrefix(symbol);
        long archivedToSeq = moved[0];
        int count = (int) moved[1];
        long hotBaseSeq = store.trimArchive(symbol);
        CompactionResult result = new CompactionResult(symbol, archivedToSeq, count,
                hotBaseSeq, true);
        log.info("[COMPACT] symbol={} 归档 {} 条后物理清除，hotBaseSeq={}；"
                        + "游标 seq < {} 将判 UNAVAILABLE",
                symbol, count, hotBaseSeq, hotBaseSeq);
        return result;
    }
}
