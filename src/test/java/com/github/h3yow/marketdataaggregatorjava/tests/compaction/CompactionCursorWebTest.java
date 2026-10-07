package com.github.h3yow.marketdataaggregatorjava.tests.compaction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.marketdataaggregatorjava.web.GlobalExceptionHandler;
import com.github.h3yow.marketdataaggregatorjava.web.MarketDataController;
import com.github.h3yow.marketdataaggregatorjava.config.MarketDataProperties;
import com.github.h3yow.marketdataaggregatorjava.domain.TradeTick;
import com.github.h3yow.marketdataaggregatorjava.service.AggregationStore;
import com.github.h3yow.marketdataaggregatorjava.service.CompactionService;
import com.github.h3yow.marketdataaggregatorjava.service.CursorService;
import com.github.h3yow.marketdataaggregatorjava.service.IngestService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 压缩游标兼容性的 HTTP 端到端测试：
 * 旧游标命中折叠区间时返回 410 + reason=COMPACTED + recovery（含两个恢复游标），
 * 用 fullReplayCursor 从头重建后逐页追平，窗口最终状态与 /candles 一致。
 */
class CompactionCursorWebTest {

    private static final Logger log = LoggerFactory.getLogger(CompactionCursorWebTest.class);

    private static final String SYM = "WEBCOMPACT";
    private static final long T0 = 1_700_000_000_000L;

    private MockMvc mvc;
    private final ObjectMapper json = new ObjectMapper();
    private AggregationStore store;
    private CursorService cursors;

    @BeforeEach
    void setUp() {
        MarketDataProperties props = new MarketDataProperties();
        props.setWindowSizeMillis(1000);
        props.setOutOfOrderToleranceMillis(5000);
        props.setCursorTtlMillis(3_600_000);
        store = new AggregationStore(props);
        cursors = new CursorService(store, props);
        IngestService ingest = new IngestService(store);
        CompactionService compaction = new CompactionService(store);
        com.github.h3yow.marketdataaggregatorjava.service.QueryService query =
                new com.github.h3yow.marketdataaggregatorjava.service.QueryService(store, props);
        com.github.h3yow.marketdataaggregatorjava.service.impl.MarketDataServiceImpl svc =
                new com.github.h3yow.marketdataaggregatorjava.service.impl.MarketDataServiceImpl(
                        ingest, query, cursors, compaction);
        mvc = MockMvcBuilders.standaloneSetup(new MarketDataController(svc))
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();

        long id = 1;
        ingest.ingest(new TradeTick(SYM, id++, T0 + 100, 10, 1));
        ingest.ingest(new TradeTick(SYM, id++, T0 + 200, 11, 2));
        ingest.ingest(new TradeTick(SYM, id++, T0 + 3500, 12, 3));
        ingest.ingest(new TradeTick(SYM, id++, T0 + 20_000, 15, 5));
    }

    private String eventsPath(String cursor) {
        String base = "/api/market-data/symbols/" + SYM + "/events?limit=2";
        return cursor == null ? base : base + "&cursor=" + cursor;
    }

    @Test
    @DisplayName("HTTP：旧游标命中折叠区间返回 410 COMPACTED+recovery，fullReplayCursor 重建与 candles 一致")
    void staleCursorReturns410WithRecoveryAndReplayConverges() throws Exception {
        // 1) 压缩前先拿一个只消费到 seq=2 的旧游标
        MvcResult first = mvc.perform(get("/api/market-data/symbols/" + SYM + "/events?limit=2"))
                .andExpect(status().isOk()).andReturn();
        JsonNode firstPage = json.readTree(first.getResponse().getContentAsString());
        String oldCursor = firstPage.path("nextCursor").asText();
        log.info("[TEST-ASSERT] 压缩前旧游标 nextCursor={}", oldCursor);

        // 2) 压缩
        MvcResult compact = mvc.perform(post("/api/market-data/symbols/" + SYM + "/compact"))
                .andExpect(status().isOk()).andReturn();
        log.info("[TEST-ASSERT] 压缩报告={}", compact.getResponse().getContentAsString());

        // 3) 用旧游标续拉：必须是 410 + COMPACTED + recovery
        MvcResult rejected = mvc.perform(get(eventsPath(oldCursor)))
                .andExpect(status().isGone()).andReturn();
        JsonNode body = json.readTree(rejected.getResponse().getContentAsString());
        log.info("[TEST-ASSERT] 拒绝响应={}", body.toString());
        org.junit.jupiter.api.Assertions.assertEquals("INVALID_CURSOR", body.path("error").asText());
        org.junit.jupiter.api.Assertions.assertEquals("COMPACTED", body.path("reason").asText());
        JsonNode recovery = body.path("recovery");
        org.junit.jupiter.api.Assertions.assertTrue(recovery.has("resumeCursor"));
        org.junit.jupiter.api.Assertions.assertTrue(recovery.has("fullReplayCursor"));
        org.junit.jupiter.api.Assertions.assertEquals(0L, recovery.path("replayFromSeq").asLong());
        org.junit.jupiter.api.Assertions.assertTrue(recovery.path("resumeFromSeq").asLong() > 0);

        // 4) 用 fullReplayCursor 逐页全量重建，收集全部窗口状态
        String cursor = recovery.path("fullReplayCursor").asText();
        java.util.Map<Long, JsonNode> replayedWindows = new java.util.HashMap<>();
        int guard = 0;
        while (guard++ < 10_000) {
            MvcResult pageResult = mvc.perform(get(eventsPath(cursor)))
                    .andExpect(status().isOk()).andReturn();
            JsonNode page = json.readTree(pageResult.getResponse().getContentAsString());
            for (JsonNode e : page.path("events")) {
                JsonNode c = e.path("candle");
                replayedWindows.put(c.path("windowStart").asLong(), c);
            }
            cursor = page.path("nextCursor").asText();
            if (!page.path("hasMore").asBoolean()) {
                break;
            }
        }

        // 5) 与区间快照查询逐窗口比对（OHLCV/gap/settled 全字段）
        MvcResult candlesResult = mvc.perform(get("/api/market-data/symbols/" + SYM + "/candles"))
                .andExpect(status().isOk()).andReturn();
        JsonNode candles = json.readTree(candlesResult.getResponse().getContentAsString());
        log.info("[TEST-ASSERT] 全量重建窗口数={}, candles 窗口数={}",
                replayedWindows.size(), candles.size());
        org.junit.jupiter.api.Assertions.assertEquals(candles.size(), replayedWindows.size());
        for (JsonNode c : candles) {
            long ws = c.path("windowStart").asLong();
            JsonNode replayed = replayedWindows.get(ws);
            org.junit.jupiter.api.Assertions.assertNotNull(replayed, "重放缺少窗口 " + ws);
            org.junit.jupiter.api.Assertions.assertEquals(c.toString(), replayed.toString(),
                    "窗口 " + ws + " 重放状态必须与 candles 完全一致");
        }
    }
}
