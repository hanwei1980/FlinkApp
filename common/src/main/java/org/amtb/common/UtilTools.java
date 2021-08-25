package org.amtb.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

@Slf4j
public class UtilTools {
    public static final String JSON_SIGN = ":";
    public static final String JSON_START_WITH = "[";

    /**
     * @param num    需要转换的十进制数
     * @param digits 保留二进制的位数（左边补零时才生效）
     * @return
     */
    public static String toBinary(int num, int digits) {
        try {
            String cover = Integer.toBinaryString(1 << digits).substring(1);
            String s = Integer.toBinaryString(num);
            return s.length() < digits ? cover.substring(s.length()) + s : s;
        } catch (Exception e) {
            log.error("", e);
            return null;
        }
    }


    /**
     * 数据降维，将结构化json转为一维map数据结构
     *
     * @param data
     * @param result
     * @return
     */
    public static boolean flatKafka(String data, HashMap<String, String> result) {
        try {
            if (StringUtils.isNotBlank(data) && StringUtils.indexOf(data, JSON_SIGN) == -1) {
                return true;
            }
            if (StringUtils.startsWith(data, JSON_START_WITH)) {
                JSONArray jsonArray = JSON.parseArray(data);
                data = jsonArray.get(0).toString();
            }
            JSONObject jsonObject = JSON.parseObject(data);
            for (String item : jsonObject.keySet()) {
                String value = jsonObject.get(item).toString();
                if (flatKafka(value, result)) {
                    result.put(item, value);
                }
            }
        } catch (Exception e) {
            log.error("org.amtb.common.UtilTools-->flatKafka Error!::", e);
            return false;
        }
        return false;
    }

    /**
     * 时间戳转换成日期格式字符串
     *
     * @param seconds 精确到秒的字符串
     * @param format
     * @return
     */
    public static String timeStamp2Date(String seconds, String format) {
        if (seconds == null || seconds.isEmpty() || seconds.equals("null")) {
            return "";
        }
        if (format == null || format.isEmpty()) {
            format = "yyyy-MM-dd HH:mm:ss";
        }
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(new Date(Long.valueOf(seconds)));
    }

    /**
     * 日期格式字符串转换成时间戳
     *
     * @param date_str 字符串日期
     * @param format   如：yyyy-MM-dd HH:mm:ss
     * @param type     true：精确到秒；false：精确到毫秒
     * @return
     */
    public static String date2TimeStamp(String date_str, String format, boolean type) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            if (type) {
                return String.valueOf(sdf.parse(date_str).getTime() / 1000);
            } else {
                return String.valueOf(sdf.parse(date_str).getTime());
            }

        } catch (Exception e) {
            log.error("org.amtb.common.UtilTools-->date2TimeStamp Error!::", e);
            return "";
        }
    }

    /**
     * 取得当前时间戳（精确到秒）
     *
     * @return
     */
    public static String timeStamp() {
        long time = System.currentTimeMillis();
        return String.valueOf(time / 1000);
    }

    /**
     * 时间计算方法，适合年，月，日，小时的加减计算
     *
     * @param dateStr  原始时间日期，字符串格式
     * @param format   格式化样式
     * @param dateType Calendar.DATE,Calendar.MONTH,Calendar.YEAR,Calendar.HOUR
     * @param seek     加减的天数，整形
     * @return 返回加减后的计算结果
     */
    public static Date seekDate(String dateStr, SimpleDateFormat format, Integer dateType, Integer seek) {
        Calendar cal = Calendar.getInstance();
        try {
            if (null == format) {
                format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }
            Date seekDate = format.parse(dateStr);
            cal.setTime(seekDate);
            cal.add(dateType, seek);
        } catch (ParseException e) {
            log.error("时间日期转换失败！", e);
        }
        return cal.getTime();
    }

    /**
     * 计算两个日期相差天数
     *
     * @param fromDate
     * @param toDate
     * @return
     */
    public static Integer daysBetween(Date fromDate, Date toDate) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(fromDate);
        long fromTime = cal.getTimeInMillis();
        cal.setTime(toDate);
        long toTime = cal.getTimeInMillis();
        if (fromTime <= toTime) {
            long betweenTime = (toTime - fromTime) / (1000 * 3600 * 24);
            return Integer.parseInt(String.valueOf(betweenTime));
        } else {
            log.error("日期差计算错误：开始日期大于结束日期！");
            log.error("fromDate=" + fromTime);
            log.error("toTime=" + toTime);
            return null;
        }
    }

    /**
     * 生成分隔线注释
     *
     * @param sign   分割线字符串
     * @param number 生成数量
     * @return
     */
    public static String printSign(String sign, Integer number) {
        StringBuilder result = new StringBuilder();
        try {
            sign = StringUtils.isNotBlank(sign) ? sign : "-";
            number = null != number ? number : 100;
            for (int i = 0; i < number; i++) {
                result.append(sign);
            }
        } catch (Exception e) {
            log.error("生成分隔线注释失败！", e);
        }
        return result.toString();
    }
}
