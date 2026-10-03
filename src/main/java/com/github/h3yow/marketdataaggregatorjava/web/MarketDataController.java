package com.github.h3yow.marketdataaggregatorjava.web;

import com.github.h3yow.marketdataaggregatorjava.domain.Candle;
import com.github.h3yow.marketdataaggregatorjava.domain.CompactionReport;
import com.github.h3yow.marketdataaggregatorjava.domain.CursorEvent;
import com.github.h3yow.marketdataaggregatorjava.domain.CursorPage;
import com.github.h3yow.marketdataaggregatorjava.domain.IngestResult;
import com.github.h3yow.marketdataaggregatorjava.domain.TradeTick;
import com.github.h3yow.marketdataaggregatorjava.service.MarketDataService;
import com.github.h3yow.marketdataaggregatorjava.web.dto.CandleResponse;
import com.github.h3yow.marketdataaggregatorjava.web.dto.CursorPageResponse;
import com.github.h3yow.marketdataaggregatorjava.web.dto.IngestResponse;
import com.github.h3yow.marketdataaggregatorjava.web.dto.TickRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 行情接入 / K 线查询 / 增量游标拉取 HTTP 入口。
 */
@RestController
@RequestMapping("/api/market-data")
public class MarketDataController {

    private final MarketDataService marketDataService;

    public MarketDataController(MarketDataService marketDataService) {
        this.marketDataService = marketDataService;
    }

    /**
     * 接入一笔逐笔行情。业务级拒绝（重复/倒退/非法）以 200 返回并在 status 字段区分，
     * 字段缺失等请求级错误返回 400。
     */
    @PostMapping("/symbols/{symbol}/ticks")
    public IngestResponse ingest(@PathVariable String symbol, @RequestBody TickRequest request) {
        if (request == null || request.tradeId() == null || request.timestamp() == null
                || request.price() == null || request.volume() == null) {
            throw new IllegalArgumentException("tradeId/timestamp/price/volume 均为必填");
        }
        TradeTick tick = new TradeTick(symbol, request.tradeId(), request.timestamp(),
                request.price(), request.volume());
        IngestResult result = marketDataService.ingest(tick);
        return new IngestResponse(result.status().name(), result.reason(), result.windowStart());
    }

    /**
     * 查询区间 K 线（含缺口标记）。from/to 为任意毫秒时间戳，自动归整到窗口起点。
     */
    @GetMapping("/symbols/{symbol}/candles")
    public List<CandleResponse> candles(@PathVariable String symbol,
                                        @RequestParam(required = false) Long from,
                                        @RequestParam(required = false) Long to) {
        return marketDataService.queryCandles(symbol, from, to).stream()
                .map(MarketDataController::toCandleResponse)
                .toList();
    }

    /**
     * 基于游标的增量拉取。首次不传 cursor；之后透传上一页 nextCursor。
     */
    @GetMapping("/symbols/{symbol}/events")
    public CursorPageResponse pull(@PathVariable String symbol,
                                   @RequestParam(required = false) String cursor,
                                   @RequestParam(defaultValue = "100") int limit) {
        CursorPage page = marketDataService.pull(symbol, cursor, limit);
        List<CursorPageResponse.EventView> views = page.events().stream()
                .map(e -> new CursorPageResponse.EventView(e.seq(), e.type().name(),
                        toCandleResponse(e.candle())))
                .toList();
        return new CursorPageResponse(page.symbol(), views, page.nextCursor(), page.hasMore());
    }

    /**
     * 压缩该标的的事件日志：折叠已结算窗口的历史事件（seq 不重编号，
     * 已签发游标全部继续有效）。返回本次压缩的统计报告。
     */
    @PostMapping("/symbols/{symbol}/compact")
    public CompactionReport compact(@PathVariable String symbol) {
        return marketDataService.compact(symbol);
    }

    private static CandleResponse toCandleResponse(Candle c) {
        return new CandleResponse(c.windowStart(), c.windowEnd(), c.open(), c.high(), c.low(),
                c.close(), c.volume(), c.gap(), c.corrected(), c.version(), c.settled());
    }
}
