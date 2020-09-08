package com.xiaotao.redis.config;

/**
 * @author wxt366@126.com
 */
public class Constants {
    /**
     * 管理库的名称
     */
    public static final String ADMIN_DATABASE = "root";

    /**
     * 过期时间管理表
     */
    public static final String EXPIRED_TABLE = "expt";

    /**
     * 滑动窗口管理表
     */
    public static final String AUTO_WINDOWS_TABLE = "awt";

    /**
     * Key的分隔符
     */
    public static final String KEY_SEPARATOR = ":";

    /**
     * 名称Key的最大长度
     */
    public static final int KEY_MAX_LENGTH = 10;
}
