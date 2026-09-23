package com.github.h3yow.marketdataaggregatorjava.service;

import com.github.h3yow.marketdataaggregatorjava.config.AggregatorProperties;
import com.github.h3yow.marketdataaggregatorjava.model.IngestDecision;
import com.github.h3yow.marketdataaggregatorjava.model.IngestResult;
import com.github.h3yow.marketdataaggregatorjava.model.Tick;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 逐笔行情接入。
 *
 * <p>处理顺序与判定规则（每一步都会打印输入行情与判定依据）：
 * <ol>
 *   <li>字段校验：symbol/tradeId 非空、price 为正、volume 为正、tradeTime 非负，否则
 *       {@link IngestDecision#REJECTED_INVALID}。</li>
 *   <li>幂等去重：同一 (symbol, tradeId) 已接受过，返回
 *       {@link IngestDecision#REJECTED_DUPLICATE}，绝不再次计入。</li>
 *   <li>迟到判定：{@code now - tradeTime > maxLateMs} 视为时间戳倒退/过期，
 *       返回 {@link IngestDecision#REJECTED_STALE}（即使 tradeId 从未见过）。</li>
 *   <li>接受：进入聚合；若落在已有数据的旧窗口为 {@link IngestDecision#ACCEPTED_LATE_FILL}，
 *       否则为 {@link IngestDecision#ACCEPTED_NEW}。</li>
 * </ol>
 */
@Service
public class TickIngestService {

    private static final Logger log = LoggerFactory.getLogger(TickIngestService.class);

    private final CandleAggregationService aggregationService;
    private final AggregatorProperties properties;

    public TickIngestService(CandleAggregationService aggregationService,
                             AggregatorProperties properties) {
        this.aggregationService = aggregationService;
        this.properties = properties;
    }

    /** 以当前系统时间接入一笔行情。 */
    public IngestResult ingest(Tick tick) {
        return ingest(tick, System.currentTimeMillis());
    }

    /**
     * 以给定参考时间接入一笔行情（便于确定性测试与"时间戳倒退"场景）。
     */
    public IngestResult ingest(Tick tick, long now) {
        String invalid = validate(tick);
        if (invalid != null) {
            log.info("[TICK-REJECT] tick={} decision={} reason={}", describe(tick),
                    IngestDecision.REJECTED_INVALID, invalid);
            return result(false, IngestDecision.REJECTED_INVALID, invalid, null, null);
        }

        long windowStart = aggregationService.windowStartOf(tick.getTradeTime());
        long lag = now - tick.getTradeTime();
        if (lag > properties.getMaxLateMs()) {
            String reason = "timestamp regression/too-old: tradeTime=" + tick.getTradeTime()
                    + " lags referenceNow=" + now + " by " + lag + "ms > maxLateMs="
                    + properties.getMaxLateMs() + "ms";
            log.info("[TICK-REJECT] tick={} decision={} windowStart={} reason={}", describe(tick),
                    IngestDecision.REJECTED_STALE, windowStart, reason);
            return result(false, IngestDecision.REJECTED_STALE, reason, windowStart, null);
        }

        // 原子操作：去重 + 写入在同一临界区内完成
        CandleAggregationService.ApplyResult applied = aggregationService.applyTick(tick);
        if (applied.getOutcome() == CandleAggregationService.ApplyOutcome.DUPLICATE) {
            String reason = "tradeId " + tick.getTradeId() + " already processed for symbol "
                    + tick.getSymbol() + "; duplicate push is not counted again";
            log.info("[TICK-REJECT] tick={} decision={} windowStart={} reason={}", describe(tick),
                    IngestDecision.REJECTED_DUPLICATE, windowStart, reason);
            return result(false, IngestDecision.REJECTED_DUPLICATE, reason, windowStart, null);
        }
        // 只有"首次创建前沿窗口"才算 NEW；对已有窗口的后续写入（前沿窗口内乱序、
        // 旧窗口迟到修正、缺口回补）统一归为可区分的 LATE_FILL。
        boolean lateFill = applied.getOutcome() != CandleAggregationService.ApplyOutcome.NEW_WINDOW;
        IngestDecision decision = lateFill ? IngestDecision.ACCEPTED_LATE_FILL : IngestDecision.ACCEPTED_NEW;
        String reason = "accepted: outcome=" + applied.getOutcome()
                + ", window=[" + applied.getWindowStart() + ","
                + (applied.getWindowStart() + properties.getWindowMs())
                + "), lag=" + lag + "ms (maxLateMs=" + properties.getMaxLateMs() + "ms)";
        log.info("[TICK-ACCEPT] tick={} decision={} windowStart={} changeSeq={} reason={}",
                describe(tick), decision, applied.getWindowStart(), applied.getChangeSeq(), reason);
        return result(true, decision, reason, applied.getWindowStart(), applied.getChangeSeq());
    }

    private String validate(Tick tick) {
        if (tick == null) {
            return "tick payload is null";
        }
        if (tick.getSymbol() == null || tick.getSymbol().isBlank()) {
            return "symbol must not be blank";
        }
        if (tick.getTradeId() == null || tick.getTradeId().isBlank()) {
            return "tradeId must not be blank";
        }
        if (tick.getPrice() <= 0) {
            return "price must be positive, got " + tick.getPrice();
        }
        if (tick.getVolume() <= 0) {
            return "volume must be positive, got " + tick.getVolume();
        }
        if (tick.getTradeTime() < 0) {
            return "tradeTime must not be negative, got " + tick.getTradeTime();
        }
        return null;
    }

    private IngestResult result(boolean accepted, IngestDecision decision, String reason,
                                Long windowStart, Long changeSeq) {
        IngestResult r = new IngestResult();
        r.setAccepted(accepted);
        r.setDecision(decision);
        r.setReason(reason);
        r.setWindowStart(windowStart);
        r.setChangeSeq(changeSeq);
        return r;
    }

    /** 打印输入行情的完整内容。 */
    private String describe(Tick tick) {
        if (tick == null) {
            return "null";
        }
        return "{symbol=" + tick.getSymbol()
                + ", tradeId=" + tick.getTradeId()
                + ", price=" + tick.getPrice()
                + ", volume=" + tick.getVolume()
                + ", tradeTime=" + tick.getTradeTime() + "}";
    }
}
