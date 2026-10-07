package com.github.h3yow.marketdataaggregatorjava.tests.concurrency;

import com.github.h3yow.marketdataaggregatorjava.config.MarketDataProperties;
import com.github.h3yow.marketdataaggregatorjava.domain.Candle;
import com.github.h3yow.marketdataaggregatorjava.domain.CursorEvent;
import com.github.h3yow.marketdataaggregatorjava.domain.CursorPage;
import com.github.h3yow.marketdataaggregatorjava.domain.IngestStatus;
import com.github.h3yow.marketdataaggregatorjava.domain.TradeTick;
import com.github.h3yow.marketdataaggregatorjava.service.AggregationStore;
import com.github.h3yow.marketdataaggregatorjava.service.CursorService;
import com.github.h3yow.marketdataaggregatorjava.service.IngestService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发接入与并发查询一致性测试。
 *
 * <p>使用确定性输入（每笔 tradeId、时间戳、量固定），多线程争抢发送同一批数据：
 * 无论调度如何，最终每个窗口的成交量必须等于“唯一 tradeId 集合”的量之和，
 * 重复推送恰好被拒一次且永不计入；查询线程在过程中看到的成交量只能单调不减，
 * 绝不会出现中间脏值。
 */
class ConcurrentReadWriteTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentReadWriteTest.class);

    private static final long T0 = 1_700_000_000_000L;

    private MarketDataProperties props() {
        MarketDataProperties props = new MarketDataProperties();
        props.setWindowSizeMillis(1000);
        props.setOutOfOrderToleranceMillis(600_000);
        return props;
    }

    @Test
    @DisplayName("多线程重复发送同一批行情：每笔唯一成交恰好计入一次，成交量不丢不重")
    void concurrentDuplicateWritesCountExactlyOnce() throws Exception {
        MarketDataProperties props = props();
        AggregationStore store = new AggregationStore(props);
        IngestService ingest = new IngestService(store);

        // 5 个窗口、每窗口 10 笔唯一成交，每笔量=tradeId，便于手算期望值
        List<TradeTick> uniqueTicks = new ArrayList<>();
        long id = 1;
        Map<Long, Long> expectedVolumeByWindow = new java.util.HashMap<>();
        for (int w = 0; w < 5; w++) {
            long volSum = 0;
            for (int k = 0; k < 10; k++) {
                long tradeId = id++;
                long offset = w * 1000L + k * 50L;
                uniqueTicks.add(new TradeTick("CONC", tradeId, T0 + offset, 10.0 + k, tradeId));
                volSum += tradeId;
            }
            expectedVolumeByWindow.put(T0 + w * 1000L, volSum);
        }

        int threads = 8;
        int rounds = 4; // 每线程把整批数据发 4 遍 -> 大量重复推送
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger duplicates = new AtomicInteger();
        AtomicInteger accepted = new AtomicInteger();
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int r = 0; r < rounds; r++) {
                    for (TradeTick tick : uniqueTicks) {
                        var result = ingest.ingest(tick);
                        if (result.status() == IngestStatus.DUPLICATE) {
                            duplicates.incrementAndGet();
                        } else {
                            accepted.incrementAndGet();
                        }
                    }
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        long uniqueCount = uniqueTicks.size();
        log.info("[TEST-ASSERT] accepted={}, duplicates={}, uniqueTicks={}",
                accepted.get(), duplicates.get(), uniqueCount);
        assertEquals(uniqueCount, accepted.get(), "唯一成交必须恰好被接受一次");
        assertEquals(uniqueCount * (threads * rounds - 1), duplicates.get(),
                "其余全部为重复推送");

        Map<Long, Candle> finalWindows = store.snapshots("CONC", null, null).stream()
                .collect(Collectors.toMap(Candle::windowStart, Function.identity()));
        for (Map.Entry<Long, Long> e : expectedVolumeByWindow.entrySet()) {
            Candle c = finalWindows.get(e.getKey());
            log.info("[TEST-ASSERT] window={} volume={} expected={} tickCount via volume check",
                    e.getKey(), c.volume(), e.getValue());
            assertEquals(e.getValue(), c.volume(),
                    "窗口 " + e.getKey() + " 成交量必须不丢不重");
        }
    }

    @Test
    @DisplayName("并发查询与回补：查询线程观察到的窗口成交量单调不减，最终值确定，事件流不重不漏")
    void concurrentReadsSeeMonotonicVolumeAndStableEvents() throws Exception {
        MarketDataProperties props = props();
        AggregationStore store = new AggregationStore(props);
        IngestService ingest = new IngestService(store);
        CursorService cursors = new CursorService(store, props);

        // 写入分两段：先发 w0、w3（中间 w1/w2 为缺口），再并发回补 + 查询
        ingest.ingest(new TradeTick("RWC", 1, T0 + 100, 10, 100));
        ingest.ingest(new TradeTick("RWC", 2, T0 + 3500, 10, 100));

        ExecutorService pool = Executors.newFixedThreadPool(6);
        CountDownLatch start = new CountDownLatch(1);
        Set<String> violations = ConcurrentHashMap.newKeySet();
        AtomicBoolean backfillDone = new AtomicBoolean(false);

        // 1 个回补线程：把缺口 w1、w2 填满，且各自重复发送
        pool.submit(() -> {
            await(start);
            for (int r = 0; r < 3; r++) {
                ingest.ingest(new TradeTick("RWC", 10, T0 + 1200, 10, 5));
                ingest.ingest(new TradeTick("RWC", 11, T0 + 2200, 10, 7));
            }
            backfillDone.set(true);
        });

        // 4 个查询线程：同一窗口的成交量必须单调不减
        for (int q = 0; q < 4; q++) {
            pool.submit(() -> {
                await(start);
                long lastW1 = -1;
                long lastW2 = -1;
                for (int i = 0; i < 2000; i++) {
                    List<Candle> snaps = store.snapshots("RWC", null, null);
                    long v1 = volumeAt(snaps, T0 + 1000);
                    long v2 = volumeAt(snaps, T0 + 2000);
                    if (v1 < lastW1 || v2 < lastW2) {
                        violations.add("窗口成交量回退: w1 " + lastW1 + "->" + v1
                                + ", w2 " + lastW2 + "->" + v2);
                    }
                    lastW1 = v1;
                    lastW2 = v2;
                }
            });
        }

        // 1 个游标拉取线程：回补线程结束前只能“临时追平”，必须在其结束后再确认一次，
        // 否则可能把回补结束前的空页当成终态而漏掉最后一批事件（修复既有 flaky）。
        List<Long> pulledSeqs = new ArrayList<>();
        pool.submit(() -> {
            await(start);
            String cursor = null;
            for (int guard = 0; guard < 1000; guard++) {
                CursorPage page = cursors.pull("RWC", cursor, 1);
                synchronized (pulledSeqs) {
                    page.events().stream().map(CursorEvent::seq).forEach(pulledSeqs::add);
                }
                cursor = page.nextCursor();
                if (!page.hasMore()) {
                    if (!backfillDone.get()) {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        continue;
                    }
                    // 回补已结束：再拉一次确认没有在两次调用之间落入新事件
                    CursorPage confirm = cursors.pull("RWC", cursor, 1);
                    synchronized (pulledSeqs) {
                        confirm.events().stream().map(CursorEvent::seq).forEach(pulledSeqs::add);
                    }
                    if (confirm.events().isEmpty() && !confirm.hasMore()) {
                        break;
                    }
                    cursor = confirm.nextCursor();
                }
            }
        });

        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        long expectedW1 = 5;
        long expectedW2 = 7;
        List<Candle> finalSnaps = store.snapshots("RWC", null, null);
        log.info("[TEST-ASSERT] w1 volume={}, w2 volume={}, violations={}",
                volumeAt(finalSnaps, T0 + 1000), volumeAt(finalSnaps, T0 + 2000), violations);
        assertEquals(expectedW1, volumeAt(finalSnaps, T0 + 1000));
        assertEquals(expectedW2, volumeAt(finalSnaps, T0 + 2000));
        assertTrue(violations.isEmpty(), "不允许出现成交量回退: " + violations);

        long maxSeq = store.maxSeq("RWC");
        List<Long> sorted = new ArrayList<>(pulledSeqs);
        java.util.Collections.sort(sorted);
        List<Long> distinct = new ArrayList<>(new HashSet<>(sorted));
        java.util.Collections.sort(distinct);
        log.info("[TEST-ASSERT] pulledSeqs count={}, distinct={}, maxSeq={}",
                sorted.size(), distinct.size(), maxSeq);
        assertEquals(maxSeq, sorted.size(), "并发拉取事件不重");
        List<Long> oneToMax = new ArrayList<>();
        for (long s = 1; s <= maxSeq; s++) {
            oneToMax.add(s);
        }
        assertEquals(oneToMax, distinct, "并发拉取事件不漏");
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static long volumeAt(List<Candle> candles, long windowStart) {
        return candles.stream()
                .filter(c -> c.windowStart() == windowStart)
                .mapToLong(Candle::volume)
                .findFirst()
                .orElse(0L);
    }
}
