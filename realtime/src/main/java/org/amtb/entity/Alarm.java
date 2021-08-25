package org.amtb.entity;

import lombok.Data;

/**
 * 将计算完的数据封装成bean对象
 */
@Data
public class Alarm {

    private String ruleRowkey;
    private String recordId; //uuid
    private String alarmSystem;
    private String alarmTime; //时间戳字符串
    private String keycode;
    private String alarmId;
    private String alarmIdName;
    private String alarmLevel;
    private String currentValue;



}