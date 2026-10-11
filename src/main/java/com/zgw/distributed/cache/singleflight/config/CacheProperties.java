package com.zgw.distributed.cache.singleflight.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 二级缓存配置项，对应 application.yml 中 cache.two-level 前缀。
 * @author 邹广文
 * @since 2026/10/3 14:23
 */
@Data
@ConfigurationProperties(prefix = "cache.two-level")
public class CacheProperties {

    /** 本地 Caffeine 最大条目数。 */
    private long localMaxSize = 10_000L;

    /** 本地 Caffeine 兜底 TTL（秒），限制副本不一致窗口上限。 */
    private long localTtlSeconds = 300L;

    /** Redis 缓存 TTL（秒）。 */
    private long redisTtlSeconds = 1800L;

    /** 本地 Striped 锁桶数量。 */
    private int localLockStripes = 1024;

    /** 单飞等待超时（秒），超时降级自行回源。 */
    private long singleFlightTimeoutSeconds = 3L;
}
