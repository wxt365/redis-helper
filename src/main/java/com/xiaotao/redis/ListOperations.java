package com.xiaotao.redis;

import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author wxt366@126.com
 */
public class ListOperations extends BaseOperations {
    public ListOperations(StringRedisTemplate redisTemplate, RedisTable redisTable) {
        super(redisTemplate, redisTable);
    }

    /**
     * 通过索引获取列表中的元素
     *
     * @param key
     * @param index
     * @return
     */
    public String index(String key, long index) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForList().index(newKey, index);
    }

    /**
     * 获取列表指定范围内的元素
     *
     * @param key
     * @param start 开始位置, 0是开始位置
     * @param end   结束位置, -1返回所有
     * @return
     */
    public List<String> range(String key, long start, long end) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForList().range(newKey, start, end);
    }

    /**
     * 存储在list头部
     *
     * @param key
     * @param value
     * @return
     */
    public Long leftPush(String key, String value) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForList().leftPush(newKey, value);
    }

    /**
     * @param key
     * @param value
     * @return
     */
    public Long leftPushAll(String key, String... value) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForList().leftPushAll(newKey, value);
    }

    /**
     * @param key
     * @param value
     * @return
     */
    public Long leftPushAll(String key, Collection<String> value) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForList().leftPushAll(newKey, value);
    }

    /**
     * 当list存在的时候才加入
     *
     * @param key
     * @param value
     * @return
     */
    public Long leftPushIfPresent(String key, String value) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForList().leftPushIfPresent(newKey, value);
    }

    /**
     * 如果pivot存在,再pivot前面添加
     *
     * @param key
     * @param pivot
     * @param value
     * @return
     */
    public Long leftPush(String key, String pivot, String value) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForList().leftPush(newKey, pivot, value);
    }

    /**
     * @param key
     * @param value
     * @return
     */
    public Long rightPush(String key, String value) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForList().rightPush(newKey, value);
    }

    /**
     * @param key
     * @param value
     * @return
     */
    public Long rightPushAll(String key, String... value) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForList().rightPushAll(newKey, value);
    }

    /**
     * @param key
     * @param value
     * @return
     */
    public Long rightPushAll(String key, Collection<String> value) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForList().rightPushAll(newKey, value);
    }

    /**
     * 为已存在的列表添加值
     *
     * @param key
     * @param value
     * @return
     */
    public Long rightPushIfPresent(String key, String value) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForList().rightPushIfPresent(newKey, value);
    }

    /**
     * 在pivot元素的右边添加值
     *
     * @param key
     * @param pivot
     * @param value
     * @return
     */
    public Long rightPush(String key, String pivot, String value) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForList().rightPush(newKey, pivot, value);
    }

    /**
     * 通过索引设置列表元素的值
     *
     * @param key
     * @param index 位置
     * @param value
     */
    public void set(String key, long index, String value) {
        String newKey = getStoreKey(key);
        redisTemplate.opsForList().set(newKey, index, value);
    }

    /**
     * 移出并获取列表的第一个元素
     *
     * @param key
     * @return 删除的元素
     */
    public String leftPop(String key) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForList().leftPop(newKey);
    }

    /**
     * 移出并获取列表的第一个元素， 如果列表没有元素会阻塞列表直到等待超时或发现可弹出元素为止
     *
     * @param key
     * @param timeout 等待时间
     * @param unit    时间单位
     * @return
     */
    public String leftPop(String key, long timeout, TimeUnit unit) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForList().leftPop(newKey, timeout, unit);
    }

    /**
     * 移除并获取列表最后一个元素
     *
     * @param key
     * @return 删除的元素
     */
    public String rightPop(String key) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForList().rightPop(newKey);
    }

    /**
     * 移出并获取列表的最后一个元素， 如果列表没有元素会阻塞列表直到等待超时或发现可弹出元素为止
     *
     * @param key
     * @param timeout 等待时间
     * @param unit    时间单位
     * @return
     */
    public String rightPop(String key, long timeout, TimeUnit unit) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForList().rightPop(newKey, timeout, unit);
    }

    /**
     * 移除列表的最后一个元素，并将该元素添加到另一个列表并返回
     *
     * @param sourceKey
     * @param destinationKey
     * @return
     */
    public String rightPopAndLeftPush(String sourceKey, String destinationKey) {
        String newSourceKey = getStoreKey(sourceKey);
        String newDestinationKey = getStoreKey(destinationKey);

        return redisTemplate.opsForList().rightPopAndLeftPush(newSourceKey,
                                                              newDestinationKey);
    }

    /**
     * 从列表中弹出一个值，将弹出的元素插入到另外一个列表中并返回它； 如果列表没有元素会阻塞列表直到等待超时或发现可弹出元素为止
     *
     * @param sourceKey
     * @param destinationKey
     * @param timeout
     * @param unit
     * @return
     */
    public String rightPopAndLeftPush(String sourceKey, String destinationKey,
                                      long timeout, TimeUnit unit) {
        String newSourceKey = getStoreKey(sourceKey);
        String newDestinationKey = getStoreKey(destinationKey);

        return redisTemplate.opsForList().rightPopAndLeftPush(newSourceKey,
                                                              newDestinationKey, timeout, unit);
    }

    /**
     * 删除集合中值等于value得元素
     *
     * @param key
     * @param index index=0, 删除所有值等于value的元素; index>0, 从头部开始删除第一个值等于value的元素;
     *              index<0, 从尾部开始删除第一个值等于value的元素;
     * @param value
     * @return
     */
    public Long remove(String key, long index, String value) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForList().remove(newKey, index, value);
    }

    /**
     * 裁剪list
     *
     * @param key
     * @param start
     * @param end
     */
    public void trim(String key, long start, long end) {
        String newKey = getStoreKey(key);
        redisTemplate.opsForList().trim(newKey, start, end);
    }

    /**
     * 获取列表长度
     *
     * @param key
     * @return
     */
    public Long size(String key) {
        String newKey = getStoreKey(key);
        return redisTemplate.opsForList().size(newKey);
    }
}
