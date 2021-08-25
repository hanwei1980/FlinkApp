package org.amtb.util;

import org.amtb.common.PropertyUtil;
import org.amtb.entity.Alarm;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;

/**
 * redis连接池工具类
 */
public class JedisUtil {
    private static JedisPoolConfig config;

    private static void initConfig() {
        config = new JedisPoolConfig();
        //redis配置
        config.setMaxIdle(Integer.parseInt(PropertyUtil.get("redis.setMaxIdle"))); //最大空闲数
        config.setMaxTotal(Integer.parseInt(PropertyUtil.get("redis.setMaxTotal"))); //最大连接数
        config.setMaxWaitMillis(Long.parseLong(PropertyUtil.get("redis.setMaxWaitMillis"))); //当池内没有返回对象时，最大等待时间
        config.setMinIdle(Integer.parseInt(PropertyUtil.get("redis.setMinIdle"))); //最小空闲数
        config.setTestOnBorrow(Boolean.getBoolean(PropertyUtil.get("redis.setTestOnBorrow"))); //测试连接是否可用
    }

    /*
     * 操作不同的redis库
     * @param dataBase
     * @return
     */
    public static JedisSentinelPool getJedisPool(String dataBase) {
        initConfig();
        //redis集群地址
        HashSet<String> sentinels = new HashSet<>();
        sentinels.add(PropertyUtil.get("redis.broker1"));
        sentinels.add(PropertyUtil.get("redis.broker2"));
        sentinels.add(PropertyUtil.get("redis.broker3"));
        String masterName = PropertyUtil.get("redis.masterName");
        int timeOut = Integer.parseInt(PropertyUtil.get("redis.timeout"));
        String password = PropertyUtil.get("redis.password");
        int baseNum = Integer.parseInt(dataBase);
        //构建连接池
        JedisSentinelPool sentinelPool = new JedisSentinelPool(masterName, sentinels, config, timeOut, password, baseNum);
        return sentinelPool;
    }

    /**
     * 获取redis连接池（无参）
     *
     * @return jedis
     */
    public static JedisSentinelPool getJedisPool() {
        JedisSentinelPool sentinelPool = getJedisPool(PropertyUtil.get("redis.database"));
        return sentinelPool;
    }

    /**
     * 获取redis连接池（有参）
     *
     * @return jedis
     */
    public static JedisSentinelPool getJedisPool(HashSet<String> brokers, String masterName, String password, int databaseNum, int timeOut) {
        initConfig();
        //构建连接池
        JedisSentinelPool sentinelPool = new JedisSentinelPool(masterName, brokers, config, timeOut, password, databaseNum);
        return sentinelPool;
    }

    /**
     * 释放资源
     *
     * @param jedis
     */
    public static void closeJedis(Jedis jedis) {
        jedis.close();
    }

    /**
     * 关闭连接池
     *
     * @param sentinelPool
     */
    public static void closeJedisPool(JedisSentinelPool sentinelPool) {
        sentinelPool.close();
    }


    /**
     * 将数据封装成元组，key对应redis的存储key，value对应hashmap值
     * 写入redis
     *
     * @param value
     * @return
     * @throws ParseException
     */
    public static Tuple2<String, HashMap<String, String>> writeRedis(Alarm value) throws ParseException {
        String period = "10";
        HashMap<String, String> valueMap = new HashMap<>();
        valueMap.put("UUID", value.getRecordId());
        valueMap.put("startTime", value.getAlarmTime());
        String endTime = TimeAndMathUtil.getIntervalTime(value.getAlarmTime(), period, "after");
        valueMap.put("endTime", endTime);
        valueMap.put("alarmSystem", value.getAlarmSystem());
        valueMap.put("alarmId", value.getAlarmId());
        valueMap.put("alarmIdName", value.getAlarmIdName());

        return new Tuple2<String, HashMap<String, String>>(value.getRuleRowkey(), valueMap);
    }
}
