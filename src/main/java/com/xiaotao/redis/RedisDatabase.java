package com.xiaotao.redis;

import com.xiaotao.redis.config.Constants;
import com.xiaotao.redis.exception.ParameterException;

import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Set;

/**
 * @author wxt366@126.com
 */
public class RedisDatabase {
    private String name;

    private RedisConnectionFactory redisConnectionFactory;

    public RedisDatabase(String name, RedisConnectionFactory redisConnectionFactory) throws ParameterException {
        if (name.contains(Constants.KEY_SEPARATOR)) {
            throw new ParameterException("Database name can't contain the String: " + Constants.KEY_SEPARATOR);
        }
        if (name.length() > Constants.KEY_MAX_LENGTH) {
            throw new ParameterException(String.format("Database name length can't longer than %d character", Constants.KEY_MAX_LENGTH));
        }

        this.name = name;
        this.redisConnectionFactory = redisConnectionFactory;
    }

    public String getName() {
        return name;
    }

    public RedisConnectionFactory getRedisConnectionFactory() {
        return redisConnectionFactory;
    }

    public void setRedisConnectionFactory(RedisConnectionFactory redisConnectionFactory) {
        this.redisConnectionFactory = redisConnectionFactory;
    }

    public RedisTable createTable(String name) throws ParameterException {
        return new RedisTable(name, this);
    }

    /**
     * 删除数据库
     */
    public void delete() {
        RedisTemplate redisTemplate = createRedisTemplate(this.redisConnectionFactory);
        Set<String> keys = redisTemplate.keys(this.name + Constants.KEY_SEPARATOR + "*");
        redisTemplate.delete(keys);
    }
    private StringRedisTemplate createRedisTemplate(RedisConnectionFactory connectionFactory) {
        StringRedisTemplate template = new StringRedisTemplate();
        template.setConnectionFactory(connectionFactory);
        template.afterPropertiesSet();
        return template;
    }

}
