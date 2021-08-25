package org.amtb.util;

import lombok.extern.slf4j.Slf4j;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * 时间处理、数学计算工具类
 */
@Slf4j
public class TimeAndMathUtil {


    /**
     * 将字符串时间戳转化为格式化时间字符串返回
     *
     * @param timestamp
     * @return time
     */
    public static String timestamp2String(String timestamp) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(Long.parseLong(timestamp));
        String time = sdf.format(date);
        return time;
    }

    /**
     * 将格式化时间转化为13位时间戳字符串返回
     *
     * @param time
     * @return timestamp
     */
    public static String string2Timestamp(String time) throws ParseException {
        if (time != null && time != "") {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return String.valueOf(sdf.parse(time).getTime());
        } else {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = new Date();
            String newTime = sdf.format(date);
            return String.valueOf(sdf.parse(newTime).getTime());
        }

    }

    /**
     * 传入一个格式化时间和一个间隔字符串（间隔单位为小时）
     * 将间隔按照分钟换算
     * 返回一个间隔的格式化时间字符串
     *
     * @param startTime
     * @param interval
     * @return
     */
    public static String getIntervalTime(String startTime, String interval, String typeName) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long startTimestamp = sdf.parse(startTime).getTime();
        long minMilliSecond = 60000L;
        int minCountOfHour = 60;
        double mins = Double.parseDouble(interval) * minCountOfHour;
        long intervalMill = (long) mins * minMilliSecond;
        if ("befour".equals(typeName)) {
            long timestamp = startTimestamp - intervalMill;
            return sdf.format(new Date(timestamp));
        } else if ("after".equals(typeName)) {
            long timestamp = startTimestamp + intervalMill;
            return sdf.format(new Date(timestamp));
        } else {
            return "null,Maybe a parameter is missing，Conversion failed";
        }
    }
}
