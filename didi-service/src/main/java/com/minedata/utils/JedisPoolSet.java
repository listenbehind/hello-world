package com.minedata.utils;

import java.util.concurrent.locks.ReentrantLock;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.minedata.service.rticHistory.RTICProperties;
import com.navinfo.mapspotter.foundation.io.util.DataSourceParams;

public class JedisPoolSet {
    private DataSourceParams params;
    private static JedisPool jedisPool;
    private static ReentrantLock lock = new ReentrantLock();



    public JedisPoolSet(DataSourceParams params) {
        super();
        this.params = params;
    }

    private static class MySingletonHandler {
        private static JedisPool instance;
        private static JedisPoolConfig config = new JedisPoolConfig();
        static {
            config.setMaxTotal(200);
            config.setMaxIdle(30);
            config.setMinIdle(8);
            config.setMaxWaitMillis(60000);
            config.setTestOnBorrow(false);
            config.setTimeBetweenEvictionRunsMillis(30000);
            config.setTestWhileIdle(true);
            config.setNumTestsPerEvictionRun(20);
            config.setMinEvictableIdleTimeMillis(60000);
            instance =
                    new JedisPool(config, RTICProperties.redis_host, RTICProperties.redis_port,
                            10000);
        }
    }


    public static JedisPool getInstance() {
        return MySingletonHandler.instance;
    }


}
