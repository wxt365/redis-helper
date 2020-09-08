package com.xiaotao.redis;

import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author wxt366@126.com
 */
public class ZSetOperations extends BaseOperations {
    public ZSetOperations(StringRedisTemplate redisTemplate, RedisTable redisTable) {
        super(redisTemplate, redisTable);
    }

    /**
     * 添加元素,有序集合是按照元素的score值由小到大排列
     *
     * @param key
     * @param value
     * @param score
     * @return
     */
    public Boolean add(String key, String value, double score) {
        String newKey = getStoreKey(key);

        return redisTemplate.opsForZSet().add(newKey, value, score);
    }

    /**
     * @param key
     * @param values
     * @return
     */
    public Long add(String key, Set<TypedTuple<String>> values) {
        String newKey = getStoreKey(key);

        return redisTemplate.opsForZSet().add(newKey, values);
    }

    /**
     * @param key
     * @param values
     * @return
     */
    public Long remove(String key, Object... values) {
        String newKey = getStoreKey(key);

        return redisTemplate.opsForZSet().remove(newKey, values);
    }

    /**
     * 增加元素的score值，并返回增加后的值
     *
     * @param key
     * @param value
     * @param delta
     * @return
     */
    public Double incrementScore(String key, String value, double delta) {
        String newKey = getStoreKey(key);

        return redisTemplate.opsForZSet().incrementScore(newKey, value, delta);
    }

    /**
     * 返回元素在集合的排名,有序集合是按照元素的score值由小到大排列
     *
     * @param key
     * @param value
     * @return 0表示第一位
     */
    public Long rank(String key, Object value) {
        String newKey = getStoreKey(key);

        return redisTemplate.opsForZSet().rank(newKey, value);
    }

    /**
     * 返回元素在集合的排名,按元素的score值由大到小排列
     *
     * @param key
     * @param value
     * @return
     */
    public Long reverseRank(String key, Object value) {
        String newKey = getStoreKey(key);

        return redisTemplate.opsForZSet().reverseRank(newKey, value);
    }

    /**
     * 获取集合的元素, 从小到大排序
     *
     * @param key
     * @param start 开始位置
     * @param end   结束位置, -1查询所有
     * @return
     */
    public Set<String> range(String key, long start, long end) {
        String newKey = getStoreKey(key);

        return redisTemplate.opsForZSet().range(newKey, start, end);
    }

    /**
     * 获取集合元素, 并且把score值也获取
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public Set<TypedTuple<String>> rangeWithScores(String key, long start, long end) {
        String newKey = getStoreKey(key);

        return redisTemplate.opsForZSet().rangeWithScores(newKey, start, end);
    }

    /**
     * 根据Score值查询集合元素
     *
     * @param key
     * @param min 最小值
     * @param max 最大值
     * @return
     */
    public Set<String> rangeByScore(String key, double min, double max) {
        String newKey = getStoreKey(key);

        return redisTemplate.opsForZSet().rangeByScore(newKey, min, max);
    }

    /**
     * 根据Score值查询集合元素, 从小到大排序
     *
     * @param key
     * @param min 最小值
     * @param max 最大值
     * @return
     */
    public Set<TypedTuple<String>> rangeByScoreWithScores(String key,
                                                          double min, double max) {
        String newKey = getStoreKey(key);

        return redisTemplate.opsForZSet().rangeByScoreWithScores(newKey, min, max);
    }

    /**
     * @param key
     * @param min
     * @param max
     * @param start
     * @param end
     * @return
     */
    public Set<TypedTuple<String>> rangeByScoreWithScores(String key,
                                                          double min, double max,
                                                          long start, long end) {
        String newKey = getStoreKey(key);

        return redisTemplate.opsForZSet().rangeByScoreWithScores(newKey, min, max,
                                                                 start, end);
    }

    /**
     * 获取集合的元素, 从大到小排序
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public Set<String> reverseRange(String key, long start, long end) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForZSet().reverseRange(newKey, start, end);
    }

    /**
     * 获取集合的元素, 从大到小排序, 并返回score值
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public Set<TypedTuple<String>> reverseRangeWithScores(String key,
                                                          long start, long end) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForZSet().reverseRangeWithScores(newKey, start,
                                                                 end);
    }

    /**
     * 根据Score值查询集合元素, 从大到小排序
     *
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Set<String> reverseRangeByScore(String key,
                                           double min, double max) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForZSet().reverseRangeByScore(newKey, min, max);
    }

    /**
     * 根据Score值查询集合元素, 从大到小排序
     *
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Set<TypedTuple<String>> reverseRangeByScoreWithScores(
            String key, double min, double max) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForZSet().reverseRangeByScoreWithScores(newKey, min, max);
    }

    /**
     * @param key
     * @param min
     * @param max
     * @param start
     * @param end
     * @return
     */
    public Set<String> reverseRangeByScore(String key,
                                           double min, double max,
                                           long start, long end) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForZSet().reverseRangeByScore(newKey, min, max, start, end);
    }

    /**
     * 根据score值获取集合元素数量
     *
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Long count(String key, double min, double max) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForZSet().count(newKey, min, max);
    }

    /**
     * 获取集合大小
     *
     * @param key
     * @return
     */
    public Long size(String key) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForZSet().size(newKey);
    }

    /**
     * 获取集合大小
     *
     * @param key
     * @return
     */
    public Long zCard(String key) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForZSet().zCard(newKey);
    }

    /**
     * 获取集合中value元素的score值
     *
     * @param key
     * @param value
     * @return
     */
    public Double score(String key, Object value) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForZSet().score(newKey, value);
    }

    /**
     * 移除指定索引位置的成员
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public Long removeRange(String key, long start, long end) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForZSet().removeRange(newKey, start, end);
    }

    /**
     * 根据指定的score值的范围来移除成员
     *
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Long removeRangeByScore(String key, double min, double max) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForZSet().removeRangeByScore(newKey, min, max);
    }

    /**
     * 获取key和otherKey的并集并存储在destKey中
     *
     * @param key
     * @param otherKey
     * @param destKey
     * @return
     */
    public Long unionAndStore(String key, String otherKey, String destKey) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForZSet().unionAndStore(key, otherKey, destKey);
    }

    /**
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
        return redisTemplate.opsForZSet()
                .unionAndStore(newKey, newOtherKeys, newDestKey);
    }

    /**
     * 交集
     *
     * @param key
     * @param otherKey
     * @param destKey
     * @return
     */
    public Long intersectAndStore(String key, String otherKey,
                                  String destKey) {
        String newKey = getStoreKey(key);
        String newOtherKey = getStoreKey(otherKey);
        String newDestKey = getStoreKey(destKey);
        return redisTemplate.opsForZSet().intersectAndStore(newKey, newOtherKey, newDestKey);
    }

    /**
     * 交集
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

        return redisTemplate.opsForZSet().intersectAndStore(newKey, newOtherKeys, newDestKey);
    }

    /**
     * @param key
     * @param options
     * @return
     */
    public Cursor<TypedTuple<String>> scan(String key, ScanOptions options) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForZSet().scan(newKey, options);
    }
}
