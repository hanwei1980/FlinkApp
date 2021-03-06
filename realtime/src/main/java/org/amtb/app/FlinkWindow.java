package org.amtb.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.amtb.common.HttpUtils;
import org.amtb.common.PropertyUtil;
import org.amtb.conversion.HbaseStandarConversion;
import org.amtb.conversion.MapConversionFunction;
import org.amtb.entity.Standar;
import org.amtb.function.CaseWhiteListFunction;
import org.amtb.source.SourceFromOracle;
import org.amtb.util.HbaseUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.hbase.sink.HBaseSinkFunction;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.*;

/**
 * Flink?????????????????????
 *
 * @author hanwei
 * @version 1.0
 * @date 2021-08-24 11:22
 */
public class FlinkWindow {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // ??????180000 ms????????????????????????????????????checkpoint????????????
        env.enableCheckpointing(5 * 60 * 1000);
        // ???????????????
        // ???????????????exactly-once ?????????????????????
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //????????????????????????????????????????????????????????????????????????????????????????????????job???????????????????????????????????????????????????job?????????????????????????????????job???
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // ??????????????????????????????10000 ms????????????checkpoint???????????????
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
        // ?????????????????????????????????????????????????????????checkpoint??????????????????
        env.getCheckpointConfig().setCheckpointTimeout(5 * 60 * 1000);
        // ??????????????????????????????????????????
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // job?????????????????????
        env.setRestartStrategy(RestartStrategies.noRestart());
        //???kafka?????????kafka???????????????  ?????????avro?????????json
        //??????kafka ???????????????
        String topic = PropertyUtil.get("read.topic");
        //??????http get????????????schema??????
        String schemaRegistryUrl = PropertyUtil.get("avro.schema");
        String schemaStr = HttpUtils.get(schemaRegistryUrl);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", PropertyUtil.get("broker.list"));
        props.setProperty("group.id", PropertyUtil.get("group.id"));
        props.setProperty("enable.auto.commit", "false");
        //kafka ?????? kerberos
      props.setProperty("security.protocol", "SASL_PLAINTEXT");
      props.setProperty("sasl.kerberos.service.name", "kafka");
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaStr);
        //??????avro????????????
        AvroDeserializationSchema avroDeserializationSchema = AvroDeserializationSchema.forGeneric(schema);
        //??????kafka????????????????????????avro??????
        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer(topic, avroDeserializationSchema, props);
        consumer.setCommitOffsetsOnCheckpoints(true);
        //?????????????????????
        consumer.setStartFromLatest();
        //TODO source
        //??????source???kafka???????????????
        DataStream<GenericData.Record> stream = env.addSource(consumer).name("ConsumerKafka");

        //??????????????????kafka???
        DataStream<Standar> kafkaStream = stream.map(new MapFunction<GenericData.Record, Standar>() {
            @Override
            public Standar map(GenericData.Record value) throws Exception {
                JSONObject jsonObj = JSON.parseObject(value.toString());
                String uuid = jsonObj.getString("uuid");
                String timestamp = jsonObj.getString("timestamp");
                String deviceEamCode = "";
                String tagsStr = jsonObj.getString("tags");
                JSONObject tagsObj = JSON.parseObject(tagsStr);
                HashMap<String, String> tagsMap = new HashMap<>();
                HashMap<String, String> metricsMap = new HashMap<>();
                ArrayList<String> dicList = new ArrayList<>();
                for (String key : tagsObj.keySet()) {
                    tagsMap.put(key, tagsObj.getString(key));
                    deviceEamCode = tagsObj.getString("device_eam_code");
                }
                String metricsStr = jsonObj.getString("metrics");
                JSONObject metricsObj = JSON.parseObject(metricsStr);
                for (String key : metricsObj.keySet()) {
                    String valueStr = metricsObj.getString(key);
                    JSONObject valueObj = JSON.parseObject(valueStr);
                    for (String valueKey : valueObj.keySet()) {
                        metricsMap.put(valueKey, valueObj.getString(valueKey));
                        dicList.add(valueKey.toUpperCase());
                    }
                }
                return new Standar(uuid, timestamp, deviceEamCode, tagsMap, metricsMap, dicList);
            }
        });
        //??????????????????????????????flink-watermark
        DataStream<Standar> dataStream = kafkaStream.assignTimestampsAndWatermarks(WatermarkStrategy.<Standar>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((iter, timestamp) -> Long.parseLong(iter.getTimestamp())));
        //??????waterMark?????????keyby???????????????
        DataStream<Standar> windowStream = dataStream.keyBy(a -> a.getDeviceEamCode()).window(TumblingEventTimeWindows.of(Time.seconds(30))).apply(new WindowFunction<Standar, Standar, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<Standar> input, Collector<Standar> out) throws Exception {
                Iterator<Standar> it = input.iterator();
                ArrayList<Standar> list = new ArrayList<>();
                while (it.hasNext()) {
                    Standar imonitor = it.next();
                    list.add(imonitor);
                }
                Collections.sort(list, (a, b) -> {
                    if (Long.parseLong(a.getTimestamp()) > Long.parseLong(b.getTimestamp())) {
                        return 1;
                    } else {
                        return -1;
                    }
                });
                for (Standar imonitor : list) {
                    out.collect(imonitor);
                }
            }
        });

        //TODO Flink-Source
        DataStream<Tuple2<String, HashMap<String, String>>> oracleStream = env.addSource(new SourceFromOracle(PropertyUtil.get("jdbc.ipop.url"), PropertyUtil.get("jdbc.ipop.user"), PropertyUtil.get("jdbc.source.password"), "sql select rule ...")).name("scanRule")
                .map(new MapConversionFunction("rule"));

        //?????????????????????
        MapStateDescriptor<String, HashMap<String, String>> whitebc = new MapStateDescriptor<>(
                "whitebc",
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.of(new TypeHint<HashMap<String, String>>() {
                })
        );
        //??????????????????
        MapStateDescriptor<String, HashMap<String, String>> rulebc = new MapStateDescriptor<>(
                "rulebc",
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.of(new TypeHint<HashMap<String, String>>() {
                })
        );

        //???????????????
        BroadcastStream<Tuple2<String, HashMap<String, String>>> broadcast = oracleStream.broadcast(whitebc);

        //??????kafka????????????????????????
        //TODO case?????????
        SingleOutputStreamOperator<Standar> standar = windowStream.connect(broadcast)
                .process(new CaseWhiteListFunction(whitebc)).name("caseWhite");

        //???????????????????????????
        OutputTag<Standar> hbaseStandarOut = new OutputTag<Standar>("hbaseStandar") {
        };
        OutputTag<Standar> oracleStandarOut = new OutputTag<Standar>("oracleStandar") {
        };
        OutputTag<Standar> copyStandarOut = new OutputTag<Standar>("copyStandar") {
        };
        //???????????????,???????????????????????????????????????hbase???????????????????????????oracle??????
        SingleOutputStreamOperator<Standar> processStandar = standar.process(new ProcessFunction<Standar, Standar>() {
            @Override
            public void processElement(Standar value, Context ctx, Collector<Standar> out) throws Exception {
                ctx.output(hbaseStandarOut, value);
                ctx.output(oracleStandarOut, value);
                ctx.output(copyStandarOut, value);
            }
        }).name("splitStandarStream");

        //????????????????????????
        DataStream<Standar> hbaseStandarStream = processStandar.getSideOutput(hbaseStandarOut);

        //get hbase schema:
        HbaseUtil hbaseUtil = new HbaseUtil();
        Tuple2<HBaseTableSchema, List<String>> schemaTuple = hbaseUtil.getStandarSchema();
        HBaseTableSchema standarSchema = schemaTuple.f0;
        List<String> keyList1 = schemaTuple.f1;

        //TODO sinkHbase(standar)
        SingleOutputStreamOperator<Tuple2<Boolean, Row>> hbaseStandar = hbaseStandarStream.map(new HbaseStandarConversion(keyList1));
        HBaseSinkFunction hbaseStandarSink = hbaseUtil.create(standarSchema, PropertyUtil.get("hbase.standar.table"));
        hbaseStandar.addSink(hbaseStandarSink).name("sink_hbase_standar").setParallelism(1);


        env.execute("standar");
    }
}
