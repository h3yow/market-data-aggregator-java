package com.github.h3yow.marketdataaggregatorjava.service.impl;

import com.github.h3yow.marketdataaggregatorjava.domain.Candle;
import com.github.h3yow.marketdataaggregatorjava.domain.CompactionReport;
import com.github.h3yow.marketdataaggregatorjava.domain.CursorPage;
import com.github.h3yow.marketdataaggregatorjava.domain.IngestResult;
import com.github.h3yow.marketdataaggregatorjava.domain.TradeTick;
import com.github.h3yow.marketdataaggregatorjava.service.CompactionService;
import com.github.h3yow.marketdataaggregatorjava.service.CursorService;
import com.github.h3yow.marketdataaggregatorjava.service.IngestService;
import com.github.h3yow.marketdataaggregatorjava.service.MarketDataService;
import com.github.h3yow.marketdataaggregatorjava.service.QueryService;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 统一门面：委托接入、查询与游标拉取三个子服务。
 */
@Service
public class MarketDataServiceImpl implements MarketDataService {

    private final IngestService ingestService;
    private final QueryService queryService;
    private final CursorService cursorService;
    private final CompactionService compactionService;

    public MarketDataServiceImpl(IngestService ingestService,
                                 QueryService queryService,
                                 CursorService cursorService,
                                 CompactionService compactionService) {
        this.ingestService = ingestService;
        this.queryService = queryService;
        this.cursorService = cursorService;
        this.compactionService = compactionService;
    }

    @Override
    public IngestResult ingest(TradeTick tick) {
        return ingestService.ingest(tick);
    }

    @Override
    public List<Candle> queryCandles(String symbol, Long fromWindowStart, Long toWindowStart) {
        return queryService.queryCandles(symbol, fromWindowStart, toWindowStart);
    }

    @Override
    public CursorPage pull(String symbol, String cursor, int limit) {
        return cursorService.pull(symbol, cursor, limit);
    }

    @Override
    public CompactionReport compact(String symbol) {
        return compactionService.compact(symbol);
    }
}
