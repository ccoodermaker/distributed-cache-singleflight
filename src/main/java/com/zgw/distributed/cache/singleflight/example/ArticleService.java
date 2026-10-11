package com.zgw.distributed.cache.singleflight.example;

import com.zgw.distributed.cache.singleflight.core.TwoLevelCache;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 文章服务示范：只针对缓存落空场景，用分布式单飞保护 Redis 和 MySQL。
 * <p>读路径：本地 Caffeine → Redis → DB，并发请求由分布式单飞收敛，只有一个节点回源。</p>
 * <p>写路径：业务方写 DB 后调用 {@link TwoLevelCache#invalidate(String)} 清本地和 Redis。</p>
 * @author 邹广文
 * @since 2026/10/4 22:40
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ArticleService {

    private final TwoLevelCache twoLevelCache;

    /**
     * 查询文章：本地缓存优先，未命中走分布式单飞回源。
     * @author 邹广文
     * @since 2026/10/5 12:41
     * @param id 文章 ID
     * @return 文章内容
     */
    public String getArticle(Long id) {
        String key = "article:" + id;
        Object value = twoLevelCache.get(key, () -> {
            // 真实场景替换为 articleMapper.selectById(id)
            log.info("回源查询数据库, id={}", id);
            return "article-content-" + id;
        });
        return value == null ? null : value.toString();
    }

    /**
     * 更新文章：写 DB 后清本地和 Redis。
     * @author 邹广文
     * @since 2026/10/5 12:45
     * @param id      文章 ID
     * @param content 新内容
     */
    @Transactional
    public void updateArticle(Long id, String content) {
        String key = "article:" + id;
        // 1. 写 DB（真实场景替换为 articleMapper.updateById）
        log.info("更新数据库, id={}, content={}", id, content);
        // 2. 清本地 + Redis
        twoLevelCache.invalidate(key);
    }

    /**
     * 删除文章：清本地和 Redis。
     * @author 邹广文
     * @since 2026/10/5 12:55
     * @param id 文章 ID
     */
    @Transactional
    public void deleteArticle(Long id) {
        String key = "article:" + id;
        // 1. 删 DB（真实场景替换为 articleMapper.deleteById）
        log.info("删除数据库记录, id={}", id);
        // 2. 清本地 + Redis
        twoLevelCache.invalidate(key);
    }
}
