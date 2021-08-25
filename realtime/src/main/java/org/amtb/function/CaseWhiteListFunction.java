package org.amtb.function;

import org.amtb.entity.Standar;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * flink 过滤白名单函数
 * 从kafka读取的原值流与白名单规则流匹配，将不属于白名单中的值置空
 */
public class CaseWhiteListFunction extends BroadcastProcessFunction<Standar, Tuple2<String, HashMap<String, String>>, Standar> {

    private MapStateDescriptor<String, HashMap<String, String>> whitebc;

    public CaseWhiteListFunction(MapStateDescriptor<String, HashMap<String, String>> whitebc) {
        this.whitebc = whitebc;
    }

    //处理数据流
    @Override
    public void processElement(Standar value, ReadOnlyContext ctx, Collector<Standar> out) throws Exception {
        ReadOnlyBroadcastState<String, HashMap<String, String>> whiteState = ctx.getBroadcastState(whitebc);
        ArrayList<String> pointList = new ArrayList<>();
        ArrayList<String> deviceList = new ArrayList<>();
        HashMap<String, String> metricsMap = value.getMetricsMap();
        //从白名单中获取所有的设备码
        for (Map.Entry<String, HashMap<String, String>> stateEntry : whiteState.immutableEntries()) {
            String deviceCode = stateEntry.getKey().split(":")[0];
            deviceList.add(deviceCode);
        }
        //筛选白名单
        for (String pointId : value.getDicList()) {
            //过滤白名单
            String whiteKey = value.getDeviceEamCode() + ":" + pointId;  //组装key
            if (whiteState.get(whiteKey) != null && !whiteState.get(whiteKey).isEmpty()) { //判空
                pointList.add(whiteState.get(whiteKey).get("UNIQUE_ID"));
            } else {
                metricsMap.put(pointId.toLowerCase(), "");
                value.setMetricsMap(metricsMap);
            }
        }
        //如果白名单存在这个设备则收集数据传递到下游
        if (deviceList.contains(value.getDeviceEamCode())) {
            value.setDicList(pointList);
            out.collect(value);
        }
    }

    //处理广播流
    @Override
    public void processBroadcastElement(Tuple2<String, HashMap<String, String>> value, Context ctx, Collector<Standar> out) throws Exception {
        BroadcastState<String, HashMap<String, String>> whiteState = ctx.getBroadcastState(whitebc);
        //更新广播redisState
        if (value != null && !value.f1.isEmpty()) {
            whiteState.put(value.f0.replace("whiteList:", ""), value.f1);
        }
    }
}
