package com.github.h3yow.marketdataaggregatorjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 端到端：接入、幂等拒绝、缺口与游标增量、非法游标 400。
 */
@SpringBootTest
@AutoConfigureMockMvc
class MarketDataControllerIT {

    private static final Logger log = LoggerFactory.getLogger(MarketDataControllerIT.class);

    @Autowired
    private MockMvc mvc;
    private final ObjectMapper json = new ObjectMapper();

    private String tickJson(String symbol, String id, long price, long vol, long t) {
        return "{\"symbol\":\"" + symbol + "\",\"tradeId\":\"" + id + "\",\"price\":" + price
                + ",\"volume\":" + vol + ",\"tradeTime\":" + t + "}";
    }

    @Test
    void ingestDedupeGapAndCursorFlowOverHttp() throws Exception {
        String symbol = "IT001";
        long base = System.currentTimeMillis();

        log.info("[HTTP-CASE] post unique tick at window base");
        mvc.perform(post("/api/market-data/ticks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(tickJson(symbol, "T1", 100, 10, base + 100)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.accepted").value(true))
                .andExpect(jsonPath("$.decision").value("ACCEPTED_NEW"));

        log.info("[HTTP-CASE] post duplicate tick, must be REJECTED_DUPLICATE");
        mvc.perform(post("/api/market-data/ticks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(tickJson(symbol, "T1", 999, 999, base + 200)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.accepted").value(false))
                .andExpect(jsonPath("$.decision").value("REJECTED_DUPLICATE"));

        log.info("[HTTP-CASE] post invalid tick (zero price), must be REJECTED_INVALID");
        mvc.perform(post("/api/market-data/ticks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(tickJson(symbol, "BAD", 0, 1, base + 300)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.decision").value("REJECTED_INVALID"));

        log.info("[HTTP-CASE] jump to t=3050 creating two GAP windows");
        mvc.perform(post("/api/market-data/ticks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(tickJson(symbol, "T2", 200, 20, base + 3050)))
                .andExpect(jsonPath("$.decision").value("ACCEPTED_NEW"));

        MvcResult page1 = mvc.perform(get("/api/market-data/symbols/" + symbol + "/candles/pull"))
                .andExpect(status().isOk())
                .andReturn();
        JsonNode body1 = json.readTree(page1.getResponse().getContentAsString());
        log.info("[HTTP-CASE] first pull changes={}, statuses={}",
                body1.get("changes").size(),
                body1.findValues("status").stream().map(JsonNode::asText).toList());
        String cursor = body1.get("nextCursor").asText();

        // 同一游标重复拉取 => 无增量
        mvc.perform(get("/api/market-data/symbols/" + symbol + "/candles/pull")
                        .param("cursor", cursor))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.changes").isEmpty());

        // 伪造游标 => 400 INVALID_CURSOR
        log.info("[HTTP-CASE] tampered cursor must yield 400 INVALID_CURSOR");
        char flipped = cursor.charAt(0) == 'A' ? 'B' : 'A';
        String tampered = flipped + cursor.substring(1);
        mvc.perform(get("/api/market-data/symbols/" + symbol + "/candles/pull")
                        .param("cursor", tampered))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("INVALID_CURSOR"));

        // 跨标的游标 => 400
        mvc.perform(get("/api/market-data/symbols/OTHER/candles/pull")
                        .param("cursor", cursor))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("INVALID_CURSOR"));
    }
}
