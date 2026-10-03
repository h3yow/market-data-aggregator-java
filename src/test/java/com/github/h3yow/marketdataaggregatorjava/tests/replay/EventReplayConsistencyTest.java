package com.github.h3yow.marketdataaggregatorjava.tests.replay;

import com.github.h3yow.marketdataaggregatorjava.config.MarketDataProperties;
import com.github.h3yow.marketdataaggregatorjava.domain.Candle;
import com.github.h3yow.marketdataaggregatorjava.domain.CursorEvent;
import com.github.h3yow.marketdataaggregatorjava.domain.CursorPage;
import com.github.h3yow.marketdataaggregatorjava.domain.TradeTick;
import com.github.h3yow.marketdataaggregatorjava.service.AggregationStore;
import com.github.h3yow.marketdataaggregatorjava.service.CursorService;
import com.github.h3yow.marketdataaggregatorjava.service.IngestService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 事件流独立重建视图的一致性测试。
 *
 * <p>重放规则（客户端视角）：按 seq 顺序消费事件，同一窗口后到的快照覆盖先到的，
 * 最终每个窗口的 Candle（含 OHLCV、缺口、corrected、version、settled）
 * 必须与直接按区间查 K 线的快照完全一致。
 */
class EventReplayConsistencyTest {

    private static final Logger log = LoggerFactory.getLogger(EventReplayConsistencyTest.class);

    private static final String SYM = "REPLAY";
    private static final long T0 = 1_700_000_000_000L;
    private static final long W = 1000;
    private static final long TOL = 5_000;

    private AggregationStore store;
    private CursorService cursorService;
    private IngestService ingestService;

    @BeforeEach
    void setUp() {
        MarketDataProperties props = new MarketDataProperties();
        props.setWindowSizeMillis(W);
        props.setOutOfOrderToleranceMillis(TOL);
        props.setCursorTtlMillis(3_600_000);
        store = new AggregationStore(props);
        cursorService = new CursorService(store, props);
        ingestService = new IngestService(store);
    }

    private void send(long id, long offset, double price, long volume) {
        TradeTick tick = new TradeTick(SYM, id, T0 + offset, price, volume);
        log.info("[TEST-INPUT] {}", tick);
        var r = ingestService.ingest(tick);
        log.info("[TEST-DECISION] status={}, window={}, reason={}", r.status(), r.windowStart(), r.reason());
    }

    /**
     * 确定性脚本：同窗口多笔正序（WINDOW_UPDATE）→ 跨窗口缺口 → 迟到回补（CORRECTION）
     * → 大幅跳跃触发早期窗口结算（WINDOW_SETTLED）。
     */
    private void ingestScript() {
        send(1, 100, 10.0, 1);      // w0 开窗
        send(2, 200, 11.0, 2);      // w0 更新 high
        send(3, 300, 9.0, 1);       // w0 更新 low/close
        send(4, 3500, 12.0, 3);     // w3 开窗，w1/w2 缺口
        send(5, 1200, 10.5, 7);     // 迟到回补 w1（CORRECTION）
        send(6, 3600, 12.5, 1);     // w3 更新
        send(7, 20_000, 15.0, 5);   // w20 开窗，w4..w19 缺口；w0..w14 陆续结算
        send(8, 21_000, 16.0, 1);   // w21 开窗，w15 结算
    }

    /** 客户端重放：从 cursor 拉完全部事件，同一窗口后者覆盖前者。 */
    private Map<Long, Candle> replay(String cursor) {
        Map<Long, Candle> view = new HashMap<>();
        String next = cursor;
        int guard = 0;
        while (guard++ < 10_000) {
            CursorPage page = cursorService.pull(SYM, next, 3); // 小页强制多翻几页
            for (CursorEvent e : page.events()) {
                view.put(e.candle().windowStart(), e.candle());
            }
            next = page.nextCursor();
            if (!page.hasMore()) {
                break;
            }
        }
        return view;
    }

    private Map<Long, Candle> snapshotView() {
        return store.snapshots(SYM, null, null).stream()
                .collect(Collectors.toMap(Candle::windowStart, Function.identity()));
    }

    @Test
    @DisplayName("从头重放事件流：每个窗口的 OHLCV/缺口/版本/结算状态与区间快照查询完全一致")
    void replayFromStartMatchesSnapshotQuery() {
        ingestScript();

        Map<Long, Candle> replayed = replay(null);
        Map<Long, Candle> snapshot = snapshotView();
        log.info("[TEST-ASSERT] 重放窗口数={}, 快照窗口数={}", replayed.size(), snapshot.size());
        assertEquals(snapshot.size(), replayed.size(), "重放必须覆盖快照中的每个窗口，不多不少");
        for (Map.Entry<Long, Candle> e : snapshot.entrySet()) {
            Candle r = replayed.get(e.getKey());
            log.info("[TEST-ASSERT] window={} snapshot={} replayed={}", e.getKey(), e.getValue(), r);
            assertEquals(e.getValue(), r, "窗口 " + e.getKey() + " 重放结果必须与快照一致");
        }
    }

    @Test
    @DisplayName("从中间游标重放：覆盖到的窗口最终状态与快照一致；同一游标重放两次结果稳定")
    void replayFromMidCursorCoversTouchedWindowsAndIsStable() {
        ingestScript();

        // 取“w0 的 WINDOW_SETTLED 事件之后”的位置作为中间游标：
        // w0 的全部事件都已消费，尾部重放不应再出现 w0
        List<CursorEvent> all = store.readEvents(SYM, 0, 1000);
        long midSeq = all.stream()
                .filter(e -> e.type() == com.github.h3yow.marketdataaggregatorjava.domain.EventType.WINDOW_SETTLED
                        && e.candle().windowStart() == T0)
                .mapToLong(CursorEvent::seq)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("脚本应使 w0 结算"));
        String midCursor = cursorService.issue(SYM, midSeq);
        log.info("[TEST-ASSERT] 中间游标 seq={}（w0 已结算事件之后）", midSeq);

        Map<Long, Candle> tail = replay(midCursor);
        Map<Long, Candle> snapshot = snapshotView();
        assertTrue(snapshot.containsKey(T0), "快照里应有 w0");
        assertTrue(!tail.containsKey(T0), "w0 的事件已全部在游标之前，尾部重放不应再覆盖 w0");
        assertTrue(tail.size() < snapshot.size(), "中间游标不应覆盖全部窗口");
        for (Map.Entry<Long, Candle> e : tail.entrySet()) {
            Candle s = snapshot.get(e.getKey());
            log.info("[TEST-ASSERT] window={} snapshot={} replayedTail={}", e.getKey(), s, e.getValue());
            assertEquals(s, e.getValue(), "窗口 " + e.getKey() + " 尾部重放状态必须与快照一致");
        }

        // 同一游标重放两次：事件序列与最终视图完全一致
        Map<Long, Candle> again = replay(midCursor);
        assertEquals(tail, again, "同一游标重复重放必须稳定");
    }

    @Test
    @DisplayName("边写边拉：持续写入过程中增量重放，追平后视图与快照一致且 seq 不重不漏")
    void replayWhileWritingConvergesToSnapshot() {
        // 分段写入、分段拉取，模拟客户端长期跟随数据流
        Map<Long, Candle> view = new HashMap<>();
        long lastSeq = 0;
        String cursor = null;

        long[][] script = {
                {1, 100, 1000, 1}, {2, 200, 1100, 2}, {3, 3500, 1200, 3},
                {4, 1200, 1050, 7}, {5, 7000, 1300, 1}, {6, 7100, 1310, 2},
                {7, 20_000, 1500, 5}, {8, 2500, 1080, 1}, {9, 30_000, 1600, 1},
        };
        for (long[] row : script) {
            send(row[0], row[1], row[2] / 100.0d, row[3]);
            // 每写一笔就把新事件追平
            while (true) {
                CursorPage page = cursorService.pull(SYM, cursor, 2);
                for (CursorEvent e : page.events()) {
                    assertTrue(e.seq() > lastSeq, "事件 seq 必须严格递增，不重复: " + e.seq());
                    lastSeq = e.seq();
                    view.put(e.candle().windowStart(), e.candle());
                }
                cursor = page.nextCursor();
                if (!page.hasMore()) {
                    break;
                }
            }
        }

        Map<Long, Candle> snapshot = snapshotView();
        log.info("[TEST-ASSERT] 边写边拉最终窗口数={}, 快照窗口数={}", view.size(), snapshot.size());
        assertEquals(snapshot, view, "持续写入下边写边拉，追平后视图必须与快照完全一致");
        assertEquals(store.maxSeq(SYM), lastSeq, "必须消费到最大 seq，不漏事件");
    }
}
