package com.github.h3yow.marketdataaggregatorjava.web;

import com.github.h3yow.marketdataaggregatorjava.model.IngestResult;
import com.github.h3yow.marketdataaggregatorjava.model.PullResponse;
import com.github.h3yow.marketdataaggregatorjava.model.Tick;
import com.github.h3yow.marketdataaggregatorjava.service.CandleAggregationService;
import com.github.h3yow.marketdataaggregatorjava.service.TickIngestService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 行情接入与K线增量拉取接口。
 */
@RestController
@RequestMapping("/api/market-data")
public class MarketDataController {

    private final TickIngestService tickIngestService;
    private final CandleAggregationService candleAggregationService;

    public MarketDataController(TickIngestService tickIngestService,
                                CandleAggregationService candleAggregationService) {
        this.tickIngestService = tickIngestService;
        this.candleAggregationService = candleAggregationService;
    }

    @PostMapping("/ticks")
    public IngestResult ingest(@RequestBody Tick tick) {
        return tickIngestService.ingest(tick);
    }

    @GetMapping("/symbols/{symbol}/candles/pull")
    public PullResponse pull(@PathVariable String symbol,
                             @RequestParam(value = "cursor", required = false) String cursor) {
        return candleAggregationService.pull(symbol, cursor, System.currentTimeMillis());
    }
}
