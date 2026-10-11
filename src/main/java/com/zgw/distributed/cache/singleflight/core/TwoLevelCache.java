package com.zgw.distributed.cache.singleflight.core;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.util.concurrent.Striped;
import com.zgw.distributed.cache.singleflight.config.CacheProperties;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;

/**
 * 二级缓存核心：只针对缓存落空场景，用分布式单飞同时保护 Redis 和 MySQL。
 * <p>读路径：Caffeine 命中直接返回 → 本地 Striped 锁 → 单飞 future → 分布式锁 → 双检 Redis → 查库 → 回填。</p>
 * <p>写路径：业务方写 DB 后调用 {@link #invalidate(String)} 清本地和 Redis。</p>
 * @author 邹广文
 * @since 2026/10/4 8:35
 */
@Slf4j
@Component
public class TwoLevelCache {

    private final CacheProperties props;

    /** 本地一级缓存。 */
    private final Cache<String, Object> localCache;

    /** Redis 二级缓存。 */
    private final RedisTemplate<String, Object> redisTemplate;

    /** Redisson 分布式锁客户端。 */
    private final RedissonClient redissonClient;

    /** JVM 内按 key 分片的本地锁，防同 JVM 并发回源。 */
    private final Striped<Lock> localLocks;

    /** 单飞登记表：key → 正在执行的 CompletableFuture。 */
    private final ConcurrentHashMap<String, CompletableFuture<Object>> inflight = new ConcurrentHashMap<>();

    public TwoLevelCache(CacheProperties props,
                         Cache<String, Object> localCache,
                         RedisTemplate<String, Object> redisTemplate,
                         RedissonClient redissonClient) {
        this.props = props;
        this.localCache = localCache;
        this.redisTemplate = redisTemplate;
        this.redissonClient = redissonClient;
        this.localLocks = Striped.lazyWeakLock(props.getLocalLockStripes());
    }

    /**
     * 读取缓存：本地命中直接返回，未命中走分布式单飞回源，同时保护 Redis 和 MySQL。
     * @author 邹广文
     * @since 2026/10/4 14:40
     * @param key    缓存 key
     * @param loader 回源逻辑（查 DB）
     * @return 缓存值，可能为 null
     */
    public Object get(String key, Supplier<Object> loader) {
        // 1. 本地缓存命中，直接返回
        Object local = localCache.getIfPresent(key);
        if (local != null) {
            return local;
        }

        // 2. 登记单飞 future，所有线程共享同一个
        CompletableFuture<Object> future = inflight.computeIfAbsent(key,
                k -> new CompletableFuture<>());

        // 3. 强引用本地锁，防 weak lock 被 GC
        Lock localLock = localLocks.get(key);
        // 4. 尝试拿本地锁，拿不到说明同 JVM 已有线程在回源，等 future 即可
        if (!localLock.tryLock()) {
            return awaitFuture(key, future, loader);
        }
        try {
            // 5. 双重检查：可能其他线程已回填
            Object recheck = localCache.getIfPresent(key);
            if (recheck != null) {
                future.complete(recheck);
                return recheck;
            }
            // 6. 唯一 owner 线程走分布式单飞：抢分布式锁 → 双检 Redis → 查库 → 回填
            Object value = loadWithDistributedLock(key, loader);
            localCache.put(key, value);
            future.complete(value);
            return value;
        } catch (Exception e) {
            future.completeExceptionally(e);
            throw e;
        } finally {
            localLock.unlock();
            inflight.remove(key, future);
        }
    }

    /**
     * 等待单飞 future，超时则降级自行走分布式锁回源。
     * @author 邹广文
     * @since 2026/10/4 14:43
     * @param key    缓存 key
     * @param future 单飞 future
     * @param loader 回源逻辑
     * @return 缓存值
     */
    private Object awaitFuture(String key, CompletableFuture<Object> future, Supplier<Object> loader) {
        try {
            return future.get(props.getSingleFlightTimeoutSeconds(), TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            log.warn("单飞等待超时, 降级自行回源, key={}", key);
            Object value = loadWithDistributedLock(key, loader);
            localCache.put(key, value);
            return value;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("等待单飞结果被中断, key=" + key, e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("单飞回源失败, key=" + key, e.getCause());
        }
    }

    /**
     * 分布式单飞：抢到锁的节点双检 Redis，未命中再查库并回填，同时保护 Redis 和 MySQL。
     * @author 邹广文
     * @since 2026/10/4 14:45
     * @param key    缓存 key
     * @param loader 回源逻辑
     * @return 回源结果
     */
    private Object loadWithDistributedLock(String key, Supplier<Object> loader) {
        RLock lock = redissonClient.getLock("cache:lock:" + key);
        // 无参 lock() 启用看门狗自动续期，锁超时时间由 Redisson 配置决定
        lock.lock();
        try {
            // 双检 Redis：可能其他节点已回填
            Object redisVal = redisTemplate.opsForValue().get(key);
            if (redisVal != null) {
                return redisVal;
            }
            // 回源查库
            Object value = loader.get();
            // 写 Redis，带 TTL
            redisTemplate.opsForValue().set(key, value,
                    Duration.ofSeconds(props.getRedisTtlSeconds()));
            return value;
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    /**
     * 失效缓存：删本地 + 删 Redis。
     * @author 邹广文
     * @since 2026/10/4 14:48
     * @param key 缓存 key
     */
    public void invalidate(String key) {
        localCache.invalidate(key);
        redisTemplate.delete(key);
    }
}
