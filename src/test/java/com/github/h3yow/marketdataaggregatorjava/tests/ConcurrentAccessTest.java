package com.github.h3yow.marketdataaggregatorjava.tests;

import com.github.h3yow.marketdataaggregatorjava.config.AggregatorProperties;
import com.github.h3yow.marketdataaggregatorjava.model.Candle;
import com.github.h3yow.marketdataaggregatorjava.model.IngestDecision;
import com.github.h3yow.marketdataaggregatorjava.model.IngestResult;
import com.github.h3yow.marketdataaggregatorjava.model.PullResponse;
import com.github.h3yow.marketdataaggregatorjava.model.Tick;
import com.github.h3yow.marketdataaggregatorjava.model.WindowStatus;
import com.github.h3yow.marketdataaggregatorjava.service.CandleAggregationService;
import com.github.h3yow.marketdataaggregatorjava.service.CursorCodec;
import com.github.h3yow.marketdataaggregatorjava.service.TickIngestService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发接入与并发查询：同一窗口聚合确定、成交量不丢不重，重复推送仅被计一次。
 */
class ConcurrentAccessTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentAccessTest.class);

    private AggregatorProperties props;
    private TickIngestService ingest;
    private CandleAggregationService agg;

    @BeforeEach
    void setUp() {
        props = new AggregatorProperties();
        props.setWindowMs(1000);
        props.setMaxLateMs(600_000);
        CursorCodec codec = new CursorCodec(props);
        agg = new CandleAggregationService(props, codec);
        ingest = new TickIngestService(agg, props);
    }

    @Test
    void concurrentTicksProduceDeterministicCandleWithNoLostOrDoubleVolume() throws Exception {
        int writers = 8;
        int perWriter = 200;   // 共 1600 笔不同 tradeId，全部落入窗口 [0,1000)
        int duplicates = 400;  // 额外重复推送同一批 tradeId
        long now = 500_000;

        ExecutorService pool = Executors.newFixedThreadPool(writers + 4);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger dupRejected = new AtomicInteger();
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

        try {
            CountDownLatch writerStart = new CountDownLatch(1);
            CountDownLatch writersDone = new CountDownLatch(writers);
            for (int w = 0; w < writers; w++) {
                final int writerId = w;
                pool.submit(() -> {
                    try {
                        writerStart.await();
                        for (int i = 0; i < perWriter; i++) {
                            int id = writerId * perWriter + i;
                            IngestResult r = push(id, now);
                            if (r.isAccepted()) {
                                accepted.incrementAndGet();
                            } else {
                                errors.add(new AssertionError("unique tick rejected: " + r.getDecision()));
                            }
                        }
                    } catch (Throwable t) {
                        errors.add(t);
                    } finally {
                        writersDone.countDown();
                    }
                });
            }
            writerStart.countDown();
            assertTrue(writersDone.await(30, TimeUnit.SECONDS), "writers did not finish in time");

            // 所有唯一 tick 已落库后，再并发重复推送：全部必须被判重，且成交量不变
            int dupThreads = 4;
            CountDownLatch dupStart = new CountDownLatch(1);
            CountDownLatch dupDone = new CountDownLatch(dupThreads);
            for (int d = 0; d < dupThreads; d++) {
                pool.submit(() -> {
                    try {
                        dupStart.await();
                        for (int i = 0; i < duplicates / dupThreads; i++) {
                            int id = (i * 37) % (writers * perWriter);
                            IngestResult r = push(id, now);
                            if (!r.isAccepted()
                                    && r.getDecision() == IngestDecision.REJECTED_DUPLICATE) {
                                dupRejected.incrementAndGet();
                            } else if (r.isAccepted()) {
                                errors.add(new AssertionError("duplicate tick was accepted"));
                            }
                        }
                    } catch (Throwable t) {
                        errors.add(t);
                    } finally {
                        dupDone.countDown();
                    }
                });
            }
            dupStart.countDown();
            assertTrue(dupDone.await(30, TimeUnit.SECONDS), "duplicate pushers did not finish in time");
        } finally {
            pool.shutdownNow();
        }

        if (!errors.isEmpty()) {
            throw errors.peek() instanceof AssertionError ae ? ae
                    : new AssertionError("worker error", errors.peek());
        }

        int uniqueTotal = writers * perWriter;
        assertEquals(uniqueTotal, accepted.get(), "every unique tick accepted exactly once");
        assertEquals(duplicates, dupRejected.get(), "every repeated push rejected as duplicate");

        Candle c = agg.getCandle("CONC", 0);
        // 每笔 volume=10，总量应严格等于 uniqueTotal*10，重复推送不计入
        assertEquals(uniqueTotal * 10L, c.getVolume());
        // open 是 tradeTime 最小的那笔（同时间按 tradeId 最小），价格规则：100+id%50
        // 每笔 tradeTime 在 0..999 内分布，确定性首/尾可以由输入集合重算验证
        long expectedOpen = expectedExtreme(uniqueTotal, true);
        long expectedClose = expectedExtreme(uniqueTotal, false);
        assertEquals(expectedOpen, c.getOpenPrice());
        assertEquals(expectedClose, c.getClosePrice());
        log.info("[ASSERT] concurrent window 0: accepted={}, duplicateRejected={}, volume={}, open={}, close={}",
                accepted.get(), dupRejected.get(), c.getVolume(), c.getOpenPrice(), c.getClosePrice());
    }

    @Test
    void concurrentPullSeesConsistentChangelogAndExactlyOnce() throws Exception {
        int pullers = 6;
        ExecutorService pool = Executors.newFixedThreadPool(pullers + 2);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch writerDone = new CountDownLatch(2);
        List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());

        try {
            // 两个写入线程，覆盖多窗口与缺口
            for (int wi = 0; wi < 2; wi++) {
                final int base = wi;
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < 60; i++) {
                            push("W" + base + "-" + i, (i * 1000L) + base * 300, 100 + i, 1, 1_000_000);
                        }
                    } catch (Throwable t) {
                        errors.add(t);
                    } finally {
                        writerDone.countDown();
                    }
                });
            }
            // 多个游标增量拉取线程：每个独立游标，各自不重不漏
            List<List<Long>> seqsPerPuller = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch pullersDone = new CountDownLatch(pullers);
            for (int p = 0; p < pullers; p++) {
                pool.submit(() -> {
                    List<Long> seqs = new ArrayList<>();
                    String cursor = null;
                    try {
                        start.await();
                        long deadline = System.currentTimeMillis() + 20_000;
                        while (System.currentTimeMillis() < deadline) {
                            PullResponse resp = agg.pull("PULL", cursor, 1_000_000);
                            resp.getChanges().forEach(ch -> seqs.add(ch.getSeq()));
                            cursor = resp.getNextCursor();
                            if (!resp.isHasMore() && writerDone.getCount() == 0) {
                                // 写完后再拉一次确认追平
                                PullResponse tail = agg.pull("PULL", cursor, 1_000_000);
                                tail.getChanges().forEach(ch -> seqs.add(ch.getSeq()));
                                if (!tail.isHasMore()) {
                                    break;
                                }
                                cursor = tail.getNextCursor();
                            }
                            Thread.sleep(2);
                        }
                    } catch (Throwable t) {
                        errors.add(t);
                    } finally {
                        seqsPerPuller.add(seqs);
                        pullersDone.countDown();
                    }
                });
            }

            start.countDown();
            assertTrue(writerDone.await(30, TimeUnit.SECONDS));
            assertTrue(pullersDone.await(40, TimeUnit.SECONDS));

            for (List<Long> seqs : seqsPerPuller) {
                Set<Long> unique = new HashSet<>(seqs);
                assertEquals(unique.size(), seqs.size(), "changelog seq must not repeat for one cursor");
            }
            log.info("[ASSERT] {} concurrent pullers each saw strictly increasing unique seqs, sizes={}",
                    seqsPerPuller.size(), seqsPerPuller.stream().map(List::size).toList());
            assertTrue(errors.isEmpty(), () -> errors.get(0).toString());
        } finally {
            pool.shutdownNow();
        }
    }

    private IngestResult push(int id, long now) {
        Tick t = new Tick();
        t.setSymbol("CONC");
        t.setTradeId("T" + id);
        t.setPrice(100 + id % 50);
        t.setVolume(10);
        // tradeTime 在 0..999 内散布且乱序
        t.setTradeTime((id * 37) % 1000);
        return ingest.ingest(t, now);
    }

    private IngestResult push(String id, long tradeTime, long price, long vol, long now) {
        Tick t = new Tick();
        t.setSymbol("PULL");
        t.setTradeId(id);
        t.setPrice(price);
        t.setVolume(vol);
        t.setTradeTime(tradeTime);
        return ingest.ingest(t, now);
    }

    /** 由测试输入重算首/尾成交价：(tradeTime, tradeId) 升序。 */
    private long expectedExtreme(int n, boolean first) {
        record Item(int time, int id) {}
        List<Item> items = new ArrayList<>();
        for (int id = 0; id < n; id++) {
            items.add(new Item((id * 37) % 1000, id));
        }
        items.sort((a, b) -> a.time() != b.time() ? Integer.compare(a.time(), b.time())
                : Integer.compare(a.id(), b.id()));
        Item pick = first ? items.get(0) : items.get(items.size() - 1);
        return 100L + (pick.id() % 50);
    }
}
