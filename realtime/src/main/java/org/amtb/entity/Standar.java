package org.amtb.entity;

import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * 将kafka原值封装为bean对象
 */
@Data
public class Standar {

    private String uuid; //大数据uuid
    private String timestamp; //13位字符串时间戳
    private String deviceEamCode; //设备唯一编码
    private HashMap<String, String> tagMap; //存储信息
    private HashMap<String, String> metricsMap;
    private ArrayList<String> dicList;

    public Standar() {
    }

    public Standar(String uuid, String timestamp, String deviceEamCode, HashMap<String, String> tagMap, HashMap<String, String> metricsMap, ArrayList<String> dicList) {
        this.uuid = uuid;
        this.timestamp = timestamp;
        this.deviceEamCode = deviceEamCode;
        this.tagMap = tagMap;
        this.metricsMap = metricsMap;
        this.dicList = dicList;
    }
}
