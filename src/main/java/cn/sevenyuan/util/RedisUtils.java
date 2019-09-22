package cn.sevenyuan.util;

import com.alibaba.fastjson.JSON;
import lombok.extern.java.Log;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * 缓存 redis 工具
 * @author JingQ at 2019-09-22
 */
@Log
public class RedisUtils {

    public static final String HOST = "localhost";

    public static final int PORT = 6379;

    public static final int TIMEOUT = 100000;

    public static final JedisPool JEDIS_POOL = new JedisPool(new JedisPoolConfig(), HOST, PORT, TIMEOUT, null);

    public static final Jedis JEDIS = JEDIS_POOL.getResource();

    /**
     * 获取简单 jedis 对象
     * @return  jedis
     */
    public static Jedis getJedis() {
        return JEDIS;
    }


    public static void close() {
        JEDIS_POOL.close();
        JEDIS.close();
    }

    public static void set(String key, Object value) {
        String realValue = JSON.toJSONString(value);
        getJedis().set(key, realValue);
    }

    public static <T> T get(String key, Class<T> classType) {
        String value = getJedis().get(key);
        try {
            return JSON.parseObject(value, classType);
        } catch (Exception e) {
            log.info("无法转换对象，确认 json 对应的属性类 classType 正确");
            return null;
        }
    }
}
