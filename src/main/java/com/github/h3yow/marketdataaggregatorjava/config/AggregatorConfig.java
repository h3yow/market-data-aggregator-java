package com.github.h3yow.marketdataaggregatorjava.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * 聚合服务装配。
 */
@Configuration
@EnableConfigurationProperties(AggregatorProperties.class)
public class AggregatorConfig {
}
