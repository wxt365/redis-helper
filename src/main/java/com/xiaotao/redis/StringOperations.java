package com.xiaotao.redis;

import com.google.gson.Gson;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.StringUtils;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author wxt366@126.com
 */
public class StringOperations extends BaseOperations {
    public StringOperations(StringRedisTemplate redisTemplate, RedisTable redisTable) {
        super(redisTemplate, redisTable);
    }

    /**
     * 设置指定 key 的值
     *
     * @param key
     * @param value
     */
    public void set(String key, String value) {
        String newKey = getStoreKey(key);

        long expire = redisTable.getTableExpire();
        if (expire <= 0) {
            expire = redisTable.getAutoWindow();
        }

        if (expire > 0) {
            redisTemplate.opsForValue().set(newKey, value, expire, TimeUnit.MILLISECONDS);
        } else {
            redisTemplate.opsForValue().set(newKey, value);
        }
    }

    /**
     * 设置指定 key 的值
     *
     * @param key
     * @param value
     */
    public <T> void set(String key, T value) {
        set(key, new Gson().toJson(value));
    }

    /**
     * 将值 value 关联到 key ，并将 key 的过期时间设为 timeout
     *
     * @param key
     * @param value
     * @param timeout 过期时间
     * @param unit    时间单位, 天:TimeUnit.DAYS 小时:TimeUnit.HOURS 分钟:TimeUnit.MINUTES
     *                秒:TimeUnit.SECONDS 毫秒:TimeUnit.MILLISECONDS
     */
    public void set(String key, String value, long timeout, TimeUnit unit) {
        String newKey = getStoreKey(key);

        redisTemplate.opsForValue().set(newKey, value, timeout, unit);
    }

    /**
     * 将值 value 关联到 key ，并将 key 的过期时间设为 timeout
     *
     * @param key
     * @param value
     * @param timeout 过期时间
     * @param unit    时间单位, 天:TimeUnit.DAYS 小时:TimeUnit.HOURS 分钟:TimeUnit.MINUTES
     *                秒:TimeUnit.SECONDS 毫秒:TimeUnit.MILLISECONDS
     */
    public <T> void set(String key, T value, long timeout, TimeUnit unit) {
        redisTemplate.opsForValue().set(key, new Gson().toJson(value), timeout, unit);
    }

    /**
     * 获取指定 key 的值
     *
     * @param key
     * @return
     */
    public String get(String key) {
        String newKey = getStoreKey(key);

        if (redisTable.isUpdateExpire(newKey)) {
            redisTemplate.expire(newKey, redisTable.getAutoWindow(), TimeUnit.MILLISECONDS);
        }
        return redisTemplate.opsForValue().get(newKey);
    }

    /**
     * 获取指定 key 的值
     *
     * @param key
     * @param classOfT
     * @return
     */
    public <T> T get(String key, Class<T> classOfT) {
        String value = get(key);
        if (StringUtils.isEmpty(value)) {
            return null;
        }

        return new Gson().fromJson(value, classOfT);
    }

    /**
     * 获取指定 key 的值
     *
     * @param key
     * @param typeOfSrc
     * @return
     */
    public <T> T get(String key, Type typeOfSrc) {
        String value = get(key);
        if (StringUtils.isEmpty(value)) {
            return null;
        }

        return new Gson().fromJson(value, typeOfSrc);
    }

    /**
     * 返回 key 中字符串值的子字符
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public String getRange(String key, long start, long end) {
        String newKey = getStoreKey(key);
        if (redisTable.isUpdateExpire(newKey)) {
            redisTemplate.expire(newKey, redisTable.getAutoWindow(), TimeUnit.MILLISECONDS);
        }

        return redisTemplate.opsForValue().get(newKey, start, end);
    }

    /**
     * 将给定 key 的值设为 value ，并返回 key 的旧值(old value)
     *
     * @param key
     * @param value
     * @return
     */
    public String getAndSet(String key, String value) {
        String newKey = getStoreKey(key);
        if (redisTable.isUpdateExpire(newKey)) {
            redisTemplate.expire(newKey, redisTable.getAutoWindow(), TimeUnit.MILLISECONDS);
        }

        return redisTemplate.opsForValue().getAndSet(newKey, value);
    }

    /**
     * 对 key 所储存的字符串值，获取指定偏移量上的位(bit)
     *
     * @param key
     * @param offset
     * @return
     */
    public Boolean getBit(String key, long offset) {
        String newKey = getStoreKey(key);

        return redisTemplate.opsForValue().getBit(newKey, offset);
    }

    /**
     * 批量获取
     *
     * @param keys
     * @return
     */
    public List<String> multiGet(Collection<String> keys) {
        Stream<String> newKeys = keys.parallelStream().map(this::getStoreKey);

        newKeys.forEach(k -> {
            if (redisTable.isUpdateExpire(k)) {
                redisTemplate.expire(k, redisTable.getAutoWindow(), TimeUnit.MILLISECONDS);
            }
        });

        return redisTemplate.opsForValue().multiGet(newKeys.collect(Collectors.toList()));
    }

    /**
     * 设置ASCII码, 字符串'a'的ASCII码是97, 转为二进制是'01100001', 此方法是将二进制第offset位值变为value
     *
     * @param key
     * @param offset 偏移量
     * @param value  值,true为1, false为0
     * @return
     */
    public boolean setBit(String key, long offset, boolean value) {
        String newKey = getStoreKey(key);

        return redisTemplate.opsForValue().setBit(newKey, offset, value);
    }

    /**
     * 只有在 key 不存在时设置 key 的值
     *
     * @param key
     * @param value
     * @return 之前已经存在返回false, 不存在返回true
     */
    public boolean setIfAbsent(String key, String value) {
        String newKey = getStoreKey(key);

        return redisTemplate.opsForValue().setIfAbsent(newKey, value);
    }

    /**
     * 用 value 参数覆写给定 key 所储存的字符串值，从偏移量 offset 开始
     *
     * @param key
     * @param value
     * @param offset 从指定位置开始覆写
     */
    public void setRange(String key, String value, long offset) {
        String newKey = getStoreKey(key);

        redisTemplate.opsForValue().set(newKey, value, offset);
    }

    /**
     * 获取字符串的长度
     *
     * @param key
     * @return
     */
    public Long size(String key) {
        String newKey = getStoreKey(key);

        return redisTemplate.opsForValue().size(newKey);
    }

    /**
     * 批量添加
     *
     * @param maps
     */
    public void multiSet(Map<String, String> maps) {
        Map<String, String> newMaps = new HashMap<>(maps.size());
        for (Map.Entry<String, String> entry : maps.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            newMaps.put(getStoreKey(key), value);
        }
        maps = null;

        redisTemplate.opsForValue().multiSet(newMaps);
    }

    /**
     * 同时设置一个或多个 key-value 对，当且仅当所有给定 key 都不存在
     *
     * @param maps
     * @return 之前已经存在返回false, 不存在返回true
     */
    public boolean multiSetIfAbsent(Map<String, String> maps) {
        Map<String, String> newMaps = new HashMap<>(maps.size());
        for (Map.Entry<String, String> entry : maps.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            newMaps.put(getStoreKey(key), value);
        }
        maps = null;

        return redisTemplate.opsForValue().multiSetIfAbsent(newMaps);
    }

    /**
     * 增加(自增长), 负数则为自减
     *
     * @param key
     * @param increment
     * @return
     */
    public Long incrBy(String key, long increment) {
        String newKey = getStoreKey(key);
        if (redisTable.isUpdateExpire(newKey)) {
            redisTemplate.expire(newKey, redisTable.getAutoWindow(), TimeUnit.MILLISECONDS);
        }
        return redisTemplate.opsForValue().increment(newKey, increment);
    }

    /**
     * @param key
     * @param increment
     * @return
     */
    public Double incrByFloat(String key, double increment) {
        String newKey = getStoreKey(key);
        if (redisTable.isUpdateExpire(newKey)) {
            redisTemplate.expire(newKey, redisTable.getAutoWindow(), TimeUnit.MILLISECONDS);
        }
        return redisTemplate.opsForValue().increment(newKey, increment);
    }

    /**
     * 追加到末尾
     *
     * @param key
     * @param value
     * @return
     */
    public Integer append(String key, String value) {
        String newKey = getStoreKey(key);
        if (redisTable.isUpdateExpire(newKey)) {
            redisTemplate.expire(newKey, redisTable.getAutoWindow(), TimeUnit.MILLISECONDS);
        }
        return redisTemplate.opsForValue().append(newKey, value);
    }
}
