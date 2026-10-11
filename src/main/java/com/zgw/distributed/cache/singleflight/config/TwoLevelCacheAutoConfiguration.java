package com.zgw.distributed.cache.singleflight.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * 二级缓存自动装配：创建 Caffeine 本地缓存 Bean 并绑定配置。
 * @author 邹广文
 * @since 2026/10/3 14:50
 */
@Configuration
@EnableConfigurationProperties(CacheProperties.class)
public class TwoLevelCacheAutoConfiguration {

    /**
     * 本地一级缓存，带最大容量和写入后过期兜底。
     * @author 邹广文
     * @since 2026/10/3 14:51
     * @param props 缓存配置
     * @return Caffeine 缓存实例
     */
    @Bean("localCache")
    public Cache<String, Object> localCache(CacheProperties props) {
        return Caffeine.newBuilder()
                .maximumSize(props.getLocalMaxSize())
                .expireAfterWrite(Duration.ofSeconds(props.getLocalTtlSeconds()))
                .build();
    }
}
