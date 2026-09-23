package com.github.h3yow.marketdataaggregatorjava.bootstrap;

import com.github.h3yow.marketdataaggregatorjava.config.MarketDataProperties;
import com.github.h3yow.marketdataaggregatorjava.domain.IngestResult;
import com.github.h3yow.marketdataaggregatorjava.domain.TradeTick;
import com.github.h3yow.marketdataaggregatorjava.service.MarketDataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 本地确定性行情注入器。
 *
 * <p>使用固定基准时间（非墙上时钟），每次启动推送完全相同的脚本，结果可复现。
 * 脚本依次覆盖：正常推进 → 重复推送 → 窗口内乱序归位 → 超容忍倒退拒绝 →
 * 字段非法拒绝 → 跨窗口产生缺口 → 迟到数据回补缺口 → 再次跨窗口标记新缺口。
 * 可通过 {@code market-data.injector-enabled=false} 关闭。
 */
@Component
@Order(0)
public class LocalTickInjector implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(LocalTickInjector.class);

    /** 固定基准：2023-11-14T22:13:20Z，仅用于确定性演示。 */
    static final long BASE_TS = 1_700_000_000_000L;
    static final String SYMBOL = "DEMO";

    private final MarketDataService marketDataService;
    private final MarketDataProperties properties;

    public LocalTickInjector(MarketDataService marketDataService, MarketDataProperties properties) {
        this.marketDataService = marketDataService;
        this.properties = properties;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!properties.isInjectorEnabled()) {
            log.info("[INJECTOR] 已禁用 (market-data.injector-enabled=false)，跳过本地行情注入");
            return;
        }
        long t = BASE_TS;
        // 每条形如 (tradeId, 相对基准偏移ms, price, volume)
        List<long[]> script = List.of(
                new long[]{1001, 100, 1000, 1},    // 正常开窗 w0，open=10.00
                new long[]{1002, 200, 1100, 2},    // 最高 11.00，量 +2
                new long[]{1003, 300, 900, 1},     // 最低 9.00，close=9.00
                new long[]{1001, 100, 1000, 1},    // 重复推送 -> DUPLICATE
                new long[]{1004, 150, 1050, 1},    // 窗口内乱序迟到 -> 修正 w0
                new long[]{1005, -6000, 100, 1},   // 倒退 6300ms 超容忍 -> REJECTED_STALE
                new long[]{1007, 3500, 1200, 3},   // 跳到 w3，w1/w2 标缺口
                new long[]{1008, 1500, 1020, 2},   // 迟到回补缺口 w1
                new long[]{1009, 2500, 1080, 1},   // 迟到回补缺口 w2
                new long[]{1010, 5200, 1300, 4}    // 跳到 w5，w4 标缺口
        );
        log.info("[INJECTOR] 开始注入 {} 笔确定性行情，symbol={}, windowSize={}ms, tolerance={}ms",
                script.size() + 1, SYMBOL, properties.getWindowSizeMillis(),
                properties.getOutOfOrderToleranceMillis());
        for (long[] row : script) {
            TradeTick tick = new TradeTick(SYMBOL, row[0], t + row[1], row[2] / 100.0d, row[3]);
            log.info("[INJECTOR] 输入行情 {}", tick);
            marketDataService.ingest(tick);
        }
        // 单独构造一笔字段非法行情（price=0 无法用 long 编码）
        TradeTick invalid = new TradeTick(SYMBOL, 1006, t + 100, 0.0d, 1);
        log.info("[INJECTOR] 输入行情(非法) {}", invalid);
        IngestResult invalidResult = marketDataService.ingest(invalid);
        log.info("[INJECTOR] 注入完成，最终非法行情判定={}, 原因={}",
                invalidResult.status(), invalidResult.reason());
    }
}
