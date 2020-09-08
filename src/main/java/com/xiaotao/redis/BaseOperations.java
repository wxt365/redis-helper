package com.xiaotao.redis;

import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Collection;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author wxt366@126.com
 */
abstract class BaseOperations {
    protected StringRedisTemplate redisTemplate;

    protected RedisTable redisTable;

    public BaseOperations(StringRedisTemplate redisTemplate, RedisTable redisTable) {
        this.redisTemplate = redisTemplate;
        this.redisTable = redisTable;
    }

    /**
     * 获取存储的Key
     * @param key
     * @return
     */
    protected String getStoreKey(String key) {
        return redisTable.getTableKey() + key;
    }

    /**
     * 删除key
     *
     * @param key
     */
    public void delete(String key) {
        String newKey = getStoreKey(key);
        redisTemplate.delete(newKey);
    }

    /**
     * 批量删除key
     *
     * @param keys
     */
    public void delete(Collection<String> keys) {
        Collection<String> newKeys = keys.parallelStream().map(this::getStoreKey).collect(Collectors.toList());

        redisTemplate.delete(newKeys);
    }

    /**
     * 序列化key
     *
     * @param key
     * @return
     */
    public byte[] dump(String key) {
        String newKey = getStoreKey(key);
        return redisTemplate.dump(newKey);
    }

    /**
     * 是否存在key
     *
     * @param key
     * @return
     */
    public Boolean hasKey(String key) {
        String newKey = getStoreKey(key);
        return redisTemplate.hasKey(newKey);
    }

    /**
     * 设置过期时间
     *
     * @param key
     * @param timeout
     * @param unit
     * @return
     */
    public Boolean expire(String key, long timeout, TimeUnit unit) {
        String newKey = getStoreKey(key);
        return redisTemplate.expire(newKey, timeout, unit);
    }

    /**
     * 设置过期时间
     *
     * @param key
     * @param date
     * @return
     */
    public Boolean expireAt(String key, Date date) {
        String newKey = getStoreKey(key);
        return redisTemplate.expireAt(newKey, date);
    }

    /**
     * 查找匹配的key
     *
     * @param pattern
     * @return
     */
    public Set<String> keys(String pattern) {
        return redisTemplate.keys(pattern);
    }

    /**
     * 将当前数据库的 key 移动到给定的数据库 db 当中
     *
     * @param key
     * @param dbIndex
     * @return
     */
    public Boolean move(String key, int dbIndex) {
        String newKey = getStoreKey(key);
        return redisTemplate.move(newKey, dbIndex);
    }

    /**
     * 移除 key 的过期时间，key 将持久保持
     *
     * @param key
     * @return
     */
    public Boolean persist(String key) {
        String newKey = getStoreKey(key);
        return redisTemplate.persist(newKey);
    }

    /**
     * 返回 key 的剩余的过期时间
     *
     * @param key
     * @param unit
     * @return
     */
    public Long getExpire(String key, TimeUnit unit) {
        String newKey = getStoreKey(key);
        return redisTemplate.getExpire(newKey, unit);
    }

    /**
     * 返回 key 的剩余的过期时间
     *
     * @param key
     * @return
     */
    public Long getExpire(String key) {
        String newKey = getStoreKey(key);
        return redisTemplate.getExpire(newKey);
    }

    /**
     * 从当前数据库中随机返回一个 key
     *
     *
     * 注：本方法最好不要使用，因为该方法返回数据中的完整key
     * @return
     */
    public String randomKey() {
        return redisTemplate.randomKey();
    }

    /**
     * 修改 key 的名称
     *
     * @param oldKey
     * @param newKey
     */
    public void rename(String oldKey, String newKey) {
        String newOldKey = getStoreKey(oldKey);
        String newNewKey = getStoreKey(newKey);

        redisTemplate.rename(newOldKey, newNewKey);
    }

    /**
     * 仅当 newkey 不存在时，将 oldKey 改名为 newkey
     *
     * @param oldKey
     * @param newKey
     * @return
     */
    public Boolean renameIfAbsent(String oldKey, String newKey) {
        String newOldKey = getStoreKey(oldKey);
        String newNewKey = getStoreKey(newKey);
        return redisTemplate.renameIfAbsent(newOldKey, newNewKey);
    }

    /**
     * 返回 key 所储存的值的类型
     *
     * @param key
     * @return
     */
    public DataType type(String key) {
        String newKey = getStoreKey(key);
        return redisTemplate.type(newKey);
    }
}
