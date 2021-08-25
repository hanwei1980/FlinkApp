package org.amtb.conversion;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * 广播状态复用map函数，通过types标识符区分返回的数据
 */
public class MapConversionFunction implements MapFunction<HashMap<String, Object>, Tuple2<String, HashMap<String, String>>> {

    private String types; //业务标识符

    public MapConversionFunction(String types) {
        this.types = types;
    }

    @Override
    public Tuple2<String, HashMap<String, String>> map(HashMap<String, Object> data) throws Exception {
        HashMap<String, String> hashMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String valueStr = entry.getValue().toString();
            hashMap.put(entry.getKey(), valueStr);
        }
        Tuple2<String, HashMap<String, String>> resultTuple = new Tuple2<>();
        switch (types) {
            case "whiteList":
                String whiteKey = "whiteList:" + hashMap.get("EQUIPMENT") + ":" + hashMap.get("UNIQUE_ID");
                resultTuple.setFields(whiteKey, hashMap);
                break;
            case "rule":
                String ruleKey = "rule:" + hashMap.get("EQUIPMENT") + ":" + hashMap.get("UNIQUE_ID") + ":" + hashMap.get("NAME");
                resultTuple.setFields(ruleKey, hashMap);
                break;
        }
        return resultTuple;
    }
}
