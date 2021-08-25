package org.amtb.app;

import lombok.extern.slf4j.Slf4j;
import org.amtb.common.HttpUtils;
import org.amtb.common.PropertyUtil;
import org.amtb.common.UtilTools;
import org.amtb.sink.SinkAlarmToOracle;
import org.amtb.source.SourceFromOracle;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


/**
 * Flink合流，广播与分流
 *
 * @author hanwei
 * @version 1.0
 * @date 2021-08-24 09:37
 */
@Slf4j
public class FlinkBroadcastStream {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每隔10000 ms进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(100000);
        // 高级选项：
        // 设置模式为exactly-once （这是默认值）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(600000);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // job失败不自动重启
        env.setRestartStrategy(RestartStrategies.noRestart());
        // 从配置文件读取kafka连接参数
        String topic = PropertyUtil.get("topics");
        String schemaRegistryUrl = PropertyUtil.get("schema.url");

        //source数据库连接信息
        String sourceUrl = PropertyUtil.get("jdbc.source.url");
        String sourceUser = PropertyUtil.get("jdbc.source.user");
        String sourcePasswd = PropertyUtil.get("jdbc.source.password");

        // 从conferent url取schema信息，如果没有取到，从配置文件读取；
        String schemaStr = HttpUtils.get(schemaRegistryUrl);
        if (StringUtils.isEmpty(schemaStr)) {
            schemaStr = PropertyUtil.get("schema.content");
        }
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", PropertyUtil.get("bootstrap.servers"));
        props.setProperty("group.id", PropertyUtil.get("group.user.id"));
        props.setProperty("enable.auto.commit", "false");

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaStr);
        // 定义avro解码方式
        AvroDeserializationSchema avroDeserializationSchema = AvroDeserializationSchema.forGeneric(schema);
        //kafka消费者，负责接收元数据
        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer(topic, avroDeserializationSchema, props);
        //kafka生产者，负责发送报警记录
        FlinkKafkaProducer<String> alarmToKafka = new FlinkKafkaProducer<>(
                PropertyUtil.get("kfk_pub_topic"),
                new SimpleStringSchema(),
                props);
        //偏移量存储到checkpoint中
        consumer.setCommitOffsetsOnCheckpoints(true);

        //从最新开始消费
        consumer.setStartFromLatest();
        DataStream<GenericData.Record> kafkaStream = env.addSource(consumer).setParallelism(2).name("source_kafka_data");
        // 存储关联报警采集点map
        HashMap<String, String> relationAlarm = new HashMap<>();
        // 主流，从kafka获取原始数据，并降维数据结构，过滤掉离合器数据
        DataStream<HashMap<String, String>> rtAlarm = kafkaStream.map(new MapFunction<GenericData.Record, HashMap<String, String>>() {
            @Override
            public HashMap<String, String> map(GenericData.Record record) {
                HashMap<String, String> kafkaData = new HashMap<>();
                UtilTools.flatKafka(record.toString(), kafkaData);
                return kafkaData;
            }
        }).filter(dKafka -> dKafka.get("collection_point_set").length() < 2);

        //获取规则流
        DataStreamSource<HashMap<String, Object>> jdbcAlarmRuleStream = env.addSource(
                new SourceFromOracle(sourceUrl, sourceUser, sourcePasswd, "sql select source..."));

        //转换规则数据结构，由HashMap转为Tuple2
        SingleOutputStreamOperator<Tuple2<String, HashMap<String, String>>> alarmRuleStream = jdbcAlarmRuleStream.map(
                new MapFunction<HashMap<String, Object>, Tuple2<String, HashMap<String, String>>>() {
                    @Override
                    public Tuple2<String, HashMap<String, String>> map(HashMap<String, Object> ruleMap) {
                        HashMap<String, String> outRule = new HashMap<>();
                        for (Map.Entry<String, Object> entry : ruleMap.entrySet()) {
                            outRule.put(entry.getKey(), entry.getValue().toString());
                        }
                        String key = outRule.get(outRule.get("POINT_ID"));
                        return new Tuple2<>(key, outRule);
                    }
                }).name("source_oracle_rule");

        //规则流广播状态
        MapStateDescriptor<String, HashMap<String, String>> ruleBc = new MapStateDescriptor<>(
                "alarmRuleBC",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<HashMap<String, String>>() {
                })
        );
        //广播规则流
        BroadcastStream<Tuple2<String, HashMap<String, String>>> alarmRuleBroadcast = alarmRuleStream.broadcast(ruleBc);

        //合并主流和规则流并判断报警
        SingleOutputStreamOperator<String> connectedStream = rtAlarm.connect(alarmRuleBroadcast).process(
                new BroadcastProcessFunction<HashMap<String, String>, Tuple2<String, HashMap<String, String>>, String>() {
                    @Override
                    public void processElement(HashMap<String, String> kafkaData,
                                               ReadOnlyContext readOnlyContext,
                                               Collector<String> collector) {
                        try {
                            ReadOnlyBroadcastState<String, HashMap<String, String>> ruleState = readOnlyContext.getBroadcastState(ruleBc);
                            //规则key
                            String key = kafkaData.get("device_eam_code") + "|" + kafkaData.get("collection_point_code");
                            //匹配规则
                            if (ruleState.contains(key)) {
                                //获取广播流中的规则
                                HashMap<String, String> ruleMap = ruleState.get(key);
                                //根据规则判断报警，返回封装报警对象
                                String jsonString = "check alarm and return json";
                                collector.collect(jsonString);
                            }
                        } catch (Exception e) {
                            log.error("RealTimeAlarm->connectedStream->processElement error::" + e.toString());
                        }
                    }

                    //聚合广播流，将流中单条数据整合到一个map中便于规则匹配
                    @Override
                    public void processBroadcastElement(Tuple2<String, HashMap<String, String>> ruleResult,
                                                        Context context, Collector<String> collector) throws Exception {
                        //获取广播状态
                        BroadcastState<String, HashMap<String, String>> alarmRule = context.getBroadcastState(ruleBc);
                        if (ruleResult != null) {
                            alarmRule.put(ruleResult.f0, ruleResult.f1);
                        }
                    }
                }).name("connect_kafka_rule");
        //结果输出分流
        OutputTag<String> toKafkaStream = new OutputTag<String>("toKafka") {
        };
        OutputTag<String> toOracletream = new OutputTag<String>("toOracle") {
        };
        //复制流
        SingleOutputStreamOperator<String> processStream = connectedStream.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String s, Context context, Collector<String> collector) {
                context.output(toKafkaStream, s);
                context.output(toOracletream, s);
            }
        }).name("SideOutPut");
        //复制流out kafka
        DataStream<String> toKafkaOut = processStream.getSideOutput(toKafkaStream);
        toKafkaOut.addSink(alarmToKafka).name("to_kafka").setParallelism(2);

        //复制流out oracle
        DataStream<String> toOracleOut = processStream.getSideOutput(toOracletream);
        toOracleOut.addSink(new SinkAlarmToOracle()).name("to_oracle");
        env.execute();
    }

}//End
