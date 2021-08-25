package org.amtb.source;

import lombok.extern.slf4j.Slf4j;
import org.amtb.common.PropertyUtil;
import org.amtb.util.JedisUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * common source redis
 */
@Slf4j
public class SourceFromRedis extends RichSourceFunction<HashMap<String, HashMap<String, String>>> {

    private static JedisSentinelPool sentinelPool;
    private static Jedis jedis;
    private volatile boolean isRunning = true;
    private String typeName;
    private Long duration = StringUtils.isNotBlank(PropertyUtil.get("redis.duration")) ? Long.parseLong(PropertyUtil.get("duration")) : 1L;

    public SourceFromRedis() {
    }

    public SourceFromRedis(String typeName) {
        this.typeName = typeName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        sentinelPool = JedisUtil.getJedisPool();
    }

    @Override
    public void close() throws Exception {
        JedisUtil.closeJedisPool(sentinelPool);
    }

    @Override
    public void run(SourceContext<HashMap<String, HashMap<String, String>>> ctx) throws Exception {
        jedis = sentinelPool.getResource();
        while (isRunning) {
            try {
                for (String key : jedis.keys("*")) {
                    Map<String, String> value = jedis.hgetAll(key);
                    HashMap result = new HashMap<String, HashMap<String, String>>();
                    result.put(key, value);
                    ctx.collect(result);
                }
            } catch (Exception e) {
                log.error("SourceFromRedis ->run() ->", e);
            }
            //休眠一分钟
            TimeUnit.MINUTES.sleep(duration);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        if (jedis != null) {
            JedisUtil.closeJedis(jedis);
        }
    }
}
