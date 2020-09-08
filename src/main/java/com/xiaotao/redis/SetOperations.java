package com.xiaotao.redis;

import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author wxt366@126.com
 */
public class SetOperations extends BaseOperations {
    public SetOperations(StringRedisTemplate redisTemplate, RedisTable redisTable) {
        super(redisTemplate, redisTable);
    }

    /**
     * set添加元素
     *
     * @param key
     * @param values
     * @return
     */
    public Long add(String key, String... values) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForSet().add(newKey, values);
    }

    /**
     * set移除元素
     *
     * @param key
     * @param values
     * @return
     */
    public Long remove(String key, Object... values) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForSet().remove(newKey, values);
    }

    /**
     * 移除并返回集合的一个随机元素
     *
     * @param key
     * @return
     */
    public String pop(String key) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForSet().pop(newKey);
    }

    /**
     * 将元素value从一个集合移到另一个集合
     *
     * @param key
     * @param value
     * @param destKey
     * @return
     */
    public Boolean move(String key, String value, String destKey) {
        String newKey = getStoreKey(key);
        String newDestKey = getStoreKey(destKey);
        return redisTemplate.opsForSet().move(newKey, value, newDestKey);
    }

    /**
     * 获取集合的大小
     *
     * @param key
     * @return
     */
    public Long size(String key) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForSet().size(newKey);
    }

    /**
     * 判断集合是否包含value
     *
     * @param key
     * @param value
     * @return
     */
    public Boolean isMember(String key, Object value) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForSet().isMember(newKey, value);
    }

    /**
     * 获取两个集合的交集
     *
     * @param key
     * @param otherKey
     * @return
     */
    public Set<String> intersect(String key, String otherKey) {
        String newKey = getStoreKey(key);
        String newOtherKey = getStoreKey(otherKey);

        return redisTemplate.opsForSet().intersect(newKey, newOtherKey);
    }

    /**
     * 获取key集合与多个集合的交集
     *
     * @param key
     * @param otherKeys
     * @return
     */
    public Set<String> intersect(String key, Collection<String> otherKeys) {
        String newKey = getStoreKey(key);
        Collection<String> newOtherKeys = otherKeys.parallelStream().map(this::getStoreKey).collect(Collectors.toList());
        return redisTemplate.opsForSet().intersect(newKey, newOtherKeys);
    }

    /**
     * key集合与otherKey集合的交集存储到destKey集合中
     *
     * @param key
     * @param otherKey
     * @param destKey
     * @return
     */
    public Long intersectAndStore(String key, String otherKey, String destKey) {
        String newKey = getStoreKey(key);
        String newOtherKey = getStoreKey(otherKey);
        String newDestKey = getStoreKey(destKey);
        return redisTemplate.opsForSet().intersectAndStore(newKey, newOtherKey, newDestKey);
    }

    /**
     * key集合与多个集合的交集存储到destKey集合中
     *
     * @param key
     * @param otherKeys
     * @param destKey
     * @return
     */
    public Long intersectAndStore(String key, Collection<String> otherKeys,
                                  String destKey) {
        String newKey = getStoreKey(key);
        String newDestKey = getStoreKey(destKey);
        Collection<String> newOtherKeys = otherKeys.parallelStream().map(this::getStoreKey).collect(Collectors.toList());
        return redisTemplate.opsForSet().intersectAndStore(newKey, newOtherKeys, newDestKey);
    }

    /**
     * 获取两个集合的并集
     *
     * @param key
     * @param otherKey
     * @return
     */
    public Set<String> union(String key, String otherKey) {
        String newKey = getStoreKey(key);
        String newOtherKey = getStoreKey(otherKey);
        return redisTemplate.opsForSet().union(newKey, newOtherKey);
    }

    /**
     * 获取key集合与多个集合的并集
     *
     * @param key
     * @param otherKeys
     * @return
     */
    public Set<String> union(String key, Collection<String> otherKeys) {
        String newKey = getStoreKey(key);
        Collection<String> newOtherKeys = otherKeys.parallelStream().map(this::getStoreKey).collect(Collectors.toList());
        return redisTemplate.opsForSet().union(newKey, newOtherKeys);
    }

    /**
     * key集合与otherKey集合的并集存储到destKey中
     *
     * @param key
     * @param otherKey
     * @param destKey
     * @return
     */
    public Long unionAndStore(String key, String otherKey, String destKey) {
        String newKey = getStoreKey(key);
        String newOtherKey = getStoreKey(otherKey);
        String newDestKey = getStoreKey(destKey);
        return redisTemplate.opsForSet().unionAndStore(newKey, newOtherKey, newDestKey);
    }

    /**
     * key集合与多个集合的并集存储到destKey中
     *
     * @param key
     * @param otherKeys
     * @param destKey
     * @return
     */
    public Long unionAndStore(String key, Collection<String> otherKeys,
                              String destKey) {
        String newKey = getStoreKey(key);
        String newDestKey = getStoreKey(destKey);
        Collection<String> newOtherKeys = otherKeys.parallelStream().map(this::getStoreKey).collect(Collectors.toList());
        return redisTemplate.opsForSet().unionAndStore(newKey, newOtherKeys, newDestKey);
    }

    /**
     * 获取两个集合的差集
     *
     * @param key
     * @param otherKey
     * @return
     */
    public Set<String> difference(String key, String otherKey) {
        String newKey = getStoreKey(key);
        String newOtherKey = getStoreKey(otherKey);
        return redisTemplate.opsForSet().difference(newKey, newOtherKey);
    }

    /**
     * 获取key集合与多个集合的差集
     *
     * @param key
     * @param otherKeys
     * @return
     */
    public Set<String> difference(String key, Collection<String> otherKeys) {
        String newKey = getStoreKey(key);
        Collection<String> newOtherKeys = otherKeys.parallelStream().map(this::getStoreKey).collect(Collectors.toList());
        return redisTemplate.opsForSet().difference(newKey, newOtherKeys);
    }

    /**
     * key集合与otherKey集合的差集存储到destKey中
     *
     * @param key
     * @param otherKey
     * @param destKey
     * @return
     */
    public Long difference(String key, String otherKey, String destKey) {
        String newKey = getStoreKey(key);
        String newOtherKey = getStoreKey(otherKey);
        String newDestKey = getStoreKey(destKey);

        return redisTemplate.opsForSet().differenceAndStore(newKey, newOtherKey,
                                                            newDestKey);
    }

    /**
     * key集合与多个集合的差集存储到destKey中
     *
     * @param key
     * @param otherKeys
     * @param destKey
     * @return
     */
    public Long difference(String key, Collection<String> otherKeys,
                           String destKey) {
        String newKey = getStoreKey(key);
        String newDestKey = getStoreKey(destKey);
        Collection<String> newOtherKeys = otherKeys.parallelStream().map(this::getStoreKey).collect(Collectors.toList());

        return redisTemplate.opsForSet().differenceAndStore(newKey, newOtherKeys,
                                                            newDestKey);
    }

    /**
     * 获取集合所有元素
     *
     * @param key
     * @return
     */
    public Set<String> members(String key) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForSet().members(newKey);
    }

    /**
     * 随机获取集合中的一个元素
     *
     * @param key
     * @return
     */
    public String randomMember(String key) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForSet().randomMember(newKey);
    }

    /**
     * 随机获取集合中count个元素
     *
     * @param key
     * @param count
     * @return
     */
    public List<String> randomMembers(String key, long count) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForSet().randomMembers(newKey, count);
    }

    /**
     * 随机获取集合中count个元素并且去除重复的
     *
     * @param key
     * @param count
     * @return
     */
    public Set<String> distinctRandomMembers(String key, long count) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForSet().distinctRandomMembers(newKey, count);
    }

    /**
     * @param key
     * @param options
     * @return
     */
    public Cursor<String> scan(String key, ScanOptions options) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForSet().scan(newKey, options);
    }
}
