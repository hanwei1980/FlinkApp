package org.amtb.conversion;

import org.amtb.entity.Standar;
import org.amtb.util.Md5Util;
import org.amtb.util.TimeAndMathUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 将解析完的Standar对象通过map转换成hashmap格式
 * 方便写入hbase
 * sink表：
 * @author hanwei
 */
public class HbaseStandarConversion implements MapFunction<Standar, Tuple2<Boolean, Row>> {

    private List<String> keyList;

    public HbaseStandarConversion(List<String> keyList) {
        this.keyList = keyList;
    }

    @Override
    public Tuple2<Boolean, Row> map(Standar value) throws Exception {
        HashMap<String, String> hashMap = new HashMap<>();
        hashMap.put("UUID", value.getUuid());
        hashMap.put("TIMESTAMP", TimeAndMathUtil.timestamp2String(value.getTimestamp()));
        HashMap<String, String> metricsMap = value.getMetricsMap();
        HashMap<String, String> tagMap = value.getTagMap();
        for (Map.Entry<String, String> entry : metricsMap.entrySet()) {
            hashMap.put(entry.getKey().replaceAll("_", "").toUpperCase(), entry.getValue());
        }
        for (Map.Entry<String, String> entry : tagMap.entrySet()) {
            hashMap.put(entry.getKey().replaceAll("_", "").toUpperCase(), entry.getValue());
        }

//        return hashMap;
        Row row = new Row(2);
        row.setField(0, Md5Util.getmd5Rowkey(hashMap.get("DEVICECODE"), "", value.getTimestamp()));
        Row data = new Row(keyList.size());
        for (int i = 0; i < keyList.size(); i++) {
            if (hashMap.get(keyList.get(i)) != null && !"".equals(hashMap.get(keyList.get(i)))) {
                data.setField(i, hashMap.get(keyList.get(i)));
            } else {
                data.setField(i, "");
            }
        }
        row.setField(1, data);
        return new Tuple2<Boolean, Row>(true, row);
    }
}
