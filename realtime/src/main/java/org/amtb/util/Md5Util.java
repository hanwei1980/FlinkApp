package org.amtb.util;

import org.apache.commons.lang3.StringUtils;

import java.security.MessageDigest;

/**
 * MD5 utils
 *
 * @author hanwei
 * @version 1.0
 * @date 2021/3/17 15:18
 */
public class Md5Util {

    /**
     * md5加密
     *
     * @param source 要加密的数据
     * @return
     */
    public static String getMd5(String source) {
        StringBuilder builder = new StringBuilder();
        try {
            //1.获取MessageDigest对象
            MessageDigest digest = MessageDigest.getInstance("md5");
            //2.执行加密操作
            byte[] bytes = source.getBytes();
            //在MD5算法这，得到的目标字节数组的特点：长度固定为16
            byte[] targetBytes = digest.digest(bytes);
            //3.声明字符数组
            char[] characters = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
            //4.遍历targetBytes
            for (byte b : targetBytes) {
                //5.取出b的高四位的值
                //先把高四位通过右移操作拽到低四位
                int high = (b >> 4) & 15;
                //6.取出b的低四位的值
                int low = b & 15;
                //7.以high为下标从characters中取出对应的十六进制字符
                char highChar = characters[high];
                //8.以low为下标从characters中取出对应的十六进制字符
                char lowChar = characters[low];
                builder.append(highChar).append(lowChar);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return builder.toString();
    }

    /**
     * md5生成rowkey
     *
     * @param deviceEamCode
     * @param feature
     * @param timestamp
     * @return
     */
    public static String getmd5Rowkey(String deviceEamCode, String feature, String timestamp) {
        String result = "";
        try {
            StringBuilder sb = new StringBuilder();
            sb.append(deviceEamCode);
            if (StringUtils.isNotBlank(feature)) {
                sb.append("|").append(feature);
            }
            result = getMd5(sb.toString()).substring(0, 6).concat("|").concat(timestamp);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}
