package com.xiaotao.redis;

import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author wxt366@126.com
 */
public class HashOperations extends BaseOperations {
    public HashOperations(StringRedisTemplate redisTemplate, RedisTable redisTable) {
        super(redisTemplate, redisTable);
    }

    /**
     * 获取存储在哈希表中指定字段的值
     *
     * @param key
     * @param field
     * @return
     */
    public Object get(String key, String field) {
        String newKey = getStoreKey(key);

        return redisTemplate.opsForHash().get(newKey, field);
    }

    /**
     * 获取所有给定字段的值
     *
     * @param key
     * @return
     */
    public Map<Object, Object> getAll(String key) {
        String newKey = getStoreKey(key);

        return redisTemplate.opsForHash().entries(newKey);
    }

    /**
     * 获取所有给定字段的值
     *
     * @param key
     * @param fields
     * @return
     */
    public List<Object> multiGet(String key, Collection<Object> fields) {
        String newKey = getStoreKey(key);

        return redisTemplate.opsForHash().multiGet(newKey, fields);
    }

    public void put(String key, String hashKey, String value) {
        String newKey = getStoreKey(key);

        redisTemplate.opsForHash().put(newKey, hashKey, value);
    }

    public void putAll(String key, Map<String, String> maps) {
        String newKey = getStoreKey(key);

        redisTemplate.opsForHash().putAll(newKey, maps);
    }

    /**
     * 仅当hashKey不存在时才设置
     *
     * @param key
     * @param hashKey
     * @param value
     * @return
     */
    public Boolean putIfAbsent(String key, String hashKey, String value) {
        String newKey = getStoreKey(key);

        return redisTemplate.opsForHash().putIfAbsent(newKey, hashKey, value);
    }

    /**
     * 删除一个或多个哈希表字段
     *
     * @param key
     * @param fields
     * @return
     */
    public Long delete(String key, Object... fields) {
        String newKey = getStoreKey(key);

        return redisTemplate.opsForHash().delete(newKey, fields);
    }

    /**
     * 查看哈希表 key 中，指定的字段是否存在
     *
     * @param key
     * @param field
     * @return
     */
    public boolean exists(String key, String field) {
        String newKey = getStoreKey(key);

        return redisTemplate.opsForHash().hasKey(newKey, field);
    }

    /**
     * 为哈希表 key 中的指定字段的整数值加上增量 increment
     *
     * @param key
     * @param field
     * @param increment
     * @return
     */
    public Long increment(String key, Object field, long increment) {
        String newKey = getStoreKey(key);

        return redisTemplate.opsForHash().increment(newKey, field, increment);
    }

    /**
     * 为哈希表 key 中的指定字段的整数值加上增量 increment
     *
     * @param key
     * @param field
     * @param delta
     * @return
     */
    public Double increment(String key, Object field, double delta) {
        String newKey = getStoreKey(key);

        return redisTemplate.opsForHash().increment(newKey, field, delta);
    }

    /**
     * 获取所有哈希表中的字段
     *
     * @param key
     * @return
     */
    public Set<Object> hKeys(String key) {
        String newKey = getStoreKey(key);

        return redisTemplate.opsForHash().keys(newKey);
    }

    /**
     * 获取哈希表中字段的数量
     *
     * @param key
     * @return
     */
    public Long size(String key) {
        String newKey = getStoreKey(key);

        return redisTemplate.opsForHash().size(newKey);
    }

    /**
     * 获取哈希表中所有值
     *
     * @param key
     * @return
     */
    public List<Object> values(String key) {
        String newKey = getStoreKey(key);

        return redisTemplate.opsForHash().values(newKey);
    }

    /**
     * 迭代哈希表中的键值对
     *
     * @param key
     * @param options
     * @return
     */
    public Cursor<Map.Entry<Object, Object>> scan(String key, ScanOptions options) {
        String newKey = getStoreKey(key);

        return redisTemplate.opsForHash().scan(newKey, options);
    }
}
