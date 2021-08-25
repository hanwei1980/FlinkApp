package org.amtb.app;


import lombok.extern.slf4j.Slf4j;
import org.amtb.common.HttpUtils;
import org.amtb.common.PropertyUtil;
import org.amtb.common.UtilTools;
import org.amtb.entity.RealTime;
import org.amtb.sink.SinkOracle;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Properties;

/**
 * flink常规流处理
 *
 * @author hanwei
 * @version 1.0
 * @date 2021-08-24 09:37
 */
@Slf4j
public class FlinkStreaming {

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
        // 从conferent url取schema信息，如果没有取到，从配置文件读取；
        String schemaStr = HttpUtils.get(schemaRegistryUrl);
        if (StringUtils.isEmpty(schemaStr)) {
            schemaStr = PropertyUtil.get("schema.content");
        }
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", PropertyUtil.get("bootstrap.servers"));
        props.setProperty("group.id", PropertyUtil.get("group.id"));
        props.setProperty("enable.auto.commit", "false");
        //如果kafka启用Kerberos认证，需要添加这两个参数
        props.setProperty("security.protocol", "SASL_PLAINTEXT");
        props.setProperty("sasl.kerberos.service.name", "kafka");
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaStr);
        // 定义avro解码方式
        AvroDeserializationSchema avroDeserializationSchema = AvroDeserializationSchema.forGeneric(schema);
        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer(topic, avroDeserializationSchema, props);
        consumer.setCommitOffsetsOnCheckpoints(true);

        // 从最早开始消费
        consumer.setStartFromLatest();
        DataStream<GenericData.Array> stream = env.addSource(consumer);

        // 执行数据转换操作，将流中的数据flatMap拍扁之后针对每条数据
        DataStream<RealTime> realTimeDataStream =
            stream.flatMap(new FlatMapFunction<GenericData.Array, RealTime>() {
                @Override
                public void flatMap(GenericData.Array value, Collector<RealTime> out) throws Exception {
                    value.forEach(item -> {
                        HashMap<String, String> kafkaData = new HashMap<>();
                        RealTime realTime = new RealTime();
                        UtilTools.flatKafka(item.toString(), kafkaData);
                        realTime.setDeviceCode(kafkaData.get("device_code"));
                        realTime.setMachineState(kafkaData.get("machinestatus"));
                        realTime.setUuid(kafkaData.get("uuid"));
                        realTime.setProduction(Integer.parseInt(kafkaData.get("production")));
                        realTime.setActiveEnergy(Double.parseDouble(kafkaData.get("activeEnergy")));
                        realTime.setCreateTime(Long.parseLong(kafkaData.get("timestamp")));
                        out.collect(realTime);
                    });
                }
            }).setParallelism(2);
        // 结果输出oracle
        realTimeDataStream.addSink(new SinkOracle()).name("to_oracle_REALTIME").setParallelism(1);

        env.execute("RealTimeStreaming");

    }

}//
