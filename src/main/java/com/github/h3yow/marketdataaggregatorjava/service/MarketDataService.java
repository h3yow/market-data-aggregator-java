package com.github.h3yow.marketdataaggregatorjava.service;

import com.github.h3yow.marketdataaggregatorjava.domain.Candle;
import com.github.h3yow.marketdataaggregatorjava.domain.CompactionReport;
import com.github.h3yow.marketdataaggregatorjava.domain.CursorPage;
import com.github.h3yow.marketdataaggregatorjava.domain.IngestResult;
import com.github.h3yow.marketdataaggregatorjava.domain.TradeTick;

import java.util.List;

/**
 * 行情接入、K 线查询与增量游标拉取的统一服务接口。
 */
public interface MarketDataService {

    IngestResult ingest(TradeTick tick);

    List<Candle> queryCandles(String symbol, Long fromWindowStart, Long toWindowStart);

    CursorPage pull(String symbol, String cursor, int limit);

    CompactionReport compact(String symbol);
}
