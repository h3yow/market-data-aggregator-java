# market-data-aggregator-java

本地行情接入与 K 线聚合服务：本地注入逐笔成交，确定性聚合固定窗口 OHLCV，
支持乱序/重复/缺口处理与签名游标增量拉取。

- 窗口与缺口规则、接入判定、一致性保证、接口与配置项详见
  [docs/AGGREGATION_RULES.md](docs/AGGREGATION_RULES.md)。

## 构建与测试

```bash
mvn clean test
mvn spring-boot:run
```

## 快速体验

```bash
curl -X POST localhost:8080/api/market-data/ticks \
  -H 'Content-Type: application/json' \
  -d '{"symbol":"600000","tradeId":"T1","price":100,"volume":10,"tradeTime":<当前毫秒时间戳>}'

curl 'localhost:8080/api/market-data/symbols/600000/candles/pull'
```
