package com.xiaotao.redis;

import com.xiaotao.redis.config.Constants;
import com.xiaotao.redis.exception.ParameterException;

import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.TimeoutUtils;
import org.springframework.util.StringUtils;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author wxt366@126.com
 */
public class RedisTable {
    private String name;

    private RedisDatabase database;

    private StringRedisTemplate redisTemplate;

    private String tableKey;

    private long autoWindows;

    private StringOperations stringOperations;
    private HashOperations hashOperations;
    private ListOperations listOperations;
    private SetOperations setOperations;
    private ZSetOperations zSetOperations;

    public RedisTable(String name, RedisDatabase database) throws ParameterException {
        if (name.contains(Constants.KEY_SEPARATOR)) {
            throw new ParameterException("Table name can't contain the String: " + Constants.KEY_SEPARATOR);
        }
        if (name.length() > Constants.KEY_MAX_LENGTH) {
            throw new ParameterException(String.format("Table name length can't longer than %d character", Constants.KEY_MAX_LENGTH));
        }

        this.name = name;
        this.database = database;
        this.redisTemplate = createRedisTemplate(database.getRedisConnectionFactory());
        this.tableKey = this.database.getName() + Constants.KEY_SEPARATOR + this.name + Constants.KEY_SEPARATOR;
        this.autoWindows = this.readAutoWindow();


        this.stringOperations = new StringOperations(this.redisTemplate, this);
        this.hashOperations = new HashOperations(this.redisTemplate, this);
        this.listOperations = new ListOperations(this.redisTemplate, this);
        this.setOperations = new SetOperations(this.redisTemplate, this);
        this.zSetOperations = new ZSetOperations(this.redisTemplate, this);
    }

    /**
     * String类型相关操作
     * @return
     */
    public StringOperations valueOps() {
        return stringOperations;
    }

    /**
     * Hash类型相关操作
     * @return
     */
    public HashOperations hashOps() {
        return hashOperations;
    }

    /**
     * List类型相关操作
     * @return
     */
    public ListOperations listOps() {
        return listOperations;
    }

    /**
     * Set类型相关操作
     * @return
     */
    public SetOperations setOps() {
        return setOperations;
    }

    /**
     * ZSet类型相关操作
     * @return
     */
    public ZSetOperations zSetOps() {
        return zSetOperations;
    }

    private StringRedisTemplate createRedisTemplate(RedisConnectionFactory connectionFactory) {
        StringRedisTemplate template = new StringRedisTemplate();
        template.setConnectionFactory(connectionFactory);
        template.afterPropertiesSet();
        return template;
    }

    /**
     * 设置表的过期时间
     *
     * @param timeout
     * @param unit
     */
    public void setExpire(long timeout, TimeUnit unit) {
        redisTemplate.opsForValue().set(getTableExpireKey(), String.valueOf(TimeoutUtils.toMillis(timeout, unit)));
    }

    /**
     * 获取表的过期时间
     *
     * @return
     */
    public long getTableExpire() {
        String expire = redisTemplate.opsForValue().get(getTableExpireKey());

        return StringUtils.isEmpty(expire) ? 0 : Long.valueOf(expire);
    }

    /**
     * 设置表的滑动窗口时间
     *
     * @param timeout
     * @param unit
     */
    public void setAutoWindow(long timeout, TimeUnit unit) {
        this.autoWindows = TimeoutUtils.toMillis(timeout, unit);
        redisTemplate.opsForValue().set(getTableAutoWindowKey(), String.valueOf(this.autoWindows));
    }

    /**
     * 获取滑动窗口时间
     *
     * @return
     */
    public long getAutoWindow() {
        return this.autoWindows;
    }

    private long readAutoWindow() {
        String expire = redisTemplate.opsForValue().get(getTableAutoWindowKey());
        return StringUtils.isEmpty(expire) ? -1 : Long.valueOf(expire);
    }

    /**
     * 获取过期时间的Key
     *
     * @return
     */
    private String getTableExpireKey() {
        StringBuilder sb = new StringBuilder();
        sb.append(Constants.ADMIN_DATABASE);
        sb.append(Constants.KEY_SEPARATOR);
        sb.append(Constants.EXPIRED_TABLE);
        sb.append(Constants.KEY_SEPARATOR);
        sb.append(this.database.getName());
        sb.append(Constants.KEY_SEPARATOR);
        sb.append(this.name);

        return sb.toString();
    }

    /**
     * 获取过期时间的Key
     *
     * @return
     */
    private String getTableAutoWindowKey() {
        StringBuilder sb = new StringBuilder();
        sb.append(Constants.ADMIN_DATABASE);
        sb.append(Constants.KEY_SEPARATOR);
        sb.append(Constants.AUTO_WINDOWS_TABLE);
        sb.append(Constants.KEY_SEPARATOR);
        sb.append(this.database.getName());
        sb.append(Constants.KEY_SEPARATOR);
        sb.append(this.name);

        return sb.toString();
    }

    /**
     * 删除表，同时删除表中的数据
     */
    public void delete(){
        Set<String> keys = redisTemplate.keys(getTableKey() + "*");
        redisTemplate.delete(keys);
    }

    /**
     * 获取表的key
     *
     * @return
     */
    public String getTableKey() {
        return tableKey;
    }

    //    private static Map<String, Long> autoWindowUpdateCache = new Hashtable<>();

    public boolean isUpdateExpire(String key) {
        if (autoWindows <= 0) {
            return false;
        }

        return true;
    }

    public StringRedisTemplate getRedisTemplate() {
        return redisTemplate;
    }

    public void setRedisTemplate(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }
}
