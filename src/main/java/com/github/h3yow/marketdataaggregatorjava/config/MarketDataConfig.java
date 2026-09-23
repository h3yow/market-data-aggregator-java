package com.github.h3yow.marketdataaggregatorjava.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * 启用行情聚合相关配置属性。
 */
@Configuration
@EnableConfigurationProperties(MarketDataProperties.class)
public class MarketDataConfig {
}
