package org.amtb.util;

import lombok.extern.slf4j.Slf4j;
import org.amtb.common.HttpUtils;
import org.amtb.common.PropertyUtil;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.hbase.sink.HBaseSinkFunction;
import org.apache.flink.connector.hbase.sink.LegacyMutationConverter;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * hbase 工具类
 */
@Slf4j
public class HbaseUtil {

    /**
     * 创建 Flink addons插件包下封装的HbaseUpsertSinkFunction用于将数据写入hbase
     *
     * @param hBaseSchema
     * @return
     */
    public HBaseSinkFunction create(HBaseTableSchema hBaseSchema, String hTableName) {
        Configuration hBaseClientConf = initHBaseClientConfig();
        //build HbaseStandarConversion
        HBaseSinkFunction sinkFunction = new HBaseSinkFunction(
                hTableName,//HBase表名
                hBaseClientConf,//HBase Client Configuration
                new LegacyMutationConverter(hBaseSchema),//HBase schema
                Long.parseLong(PropertyUtil.get("hbase.maxsize")) * 1024 * 1024,//缓存刷新大小
                Long.parseLong(PropertyUtil.get("hbase.maxrows")),//缓存刷新行数
                Long.parseLong(PropertyUtil.get("hbase.interval"))//缓存刷新间隔
        );//字节，行，间隔时间
        return sinkFunction;
    }

    /**
     * 创建连接
     *
     * @return
     * @throws IOException
     */
    public Connection createConnection() throws IOException {
        Configuration config = initHBaseClientConfig();
        Connection connection = ConnectionFactory.createConnection(config);
        return connection;
    }

    /**
     * 构建HBase Config配置
     *
     * @return
     */
    private Configuration initHBaseClientConfig() {
        Configuration hBaseClientConf = HBaseConfiguration.create();
        String zookeeperQuorum = PropertyUtil.get("hbase.zookeeper.quorum");
        hBaseClientConf.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum);
        hBaseClientConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, PropertyUtil.get("hbase.zookeeper.point")); //2181
        hBaseClientConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, PropertyUtil.get("hbase.zookeeper.znode")); //hbase

        //认证需要的参数
        //hbase authentication is kerberos ,rpc is privacy
        String realm = getRealm(zookeeperQuorum);
        String hbaseKerberosPrincipal = PropertyUtil.get("hbase.reaml.prefix") + realm;
        String hbaseSecurityAuthentication = PropertyUtil.get("hbase.security.authentication");
        String hbaseRpcProtection = PropertyUtil.get("hbase.rpc.protection");
        String hbaseRpcTimeout = PropertyUtil.get("hbase.rpc.timeout");
        String hbaseClientTimeout = PropertyUtil.get("hbase.client.operation.timeout");
        String hbaseClientPeriod = PropertyUtil.get("hbase.client.scanner.timeout.period");

        hBaseClientConf.set("hbase.security.authentication", hbaseSecurityAuthentication);
        hBaseClientConf.set("hbase.rpc.protection", hbaseRpcProtection);
        hBaseClientConf.set("hbase.master.kerberos.principal", hbaseKerberosPrincipal);
        hBaseClientConf.set("hbase.regionserver.kerberos.principal", hbaseKerberosPrincipal);
        hBaseClientConf.set("hadoop.security.authentication", hbaseSecurityAuthentication);
        hBaseClientConf.set("hbase.rpc.timeout", hbaseRpcTimeout);
        hBaseClientConf.set("hbase.client.operation.timeout", hbaseClientTimeout);
        hBaseClientConf.set("hbase.client.scanner.timeout.period", hbaseClientPeriod);
        return hBaseClientConf;
    }

    /**
     * get realm from zookeeper quorum
     *
     * @param hosts
     * @return
     */
    private String getRealm(String hosts) {
        String[] hostArray = hosts.split(",");
        return hostArray[0].substring(hostArray[0].indexOf(".") + 1).toUpperCase();
    }


    /**
     * 获取hbase表的schema对象
     *
     * @return
     */
    public Tuple2<HBaseTableSchema, List<String>> getStandarSchema() {
        String url = PropertyUtil.get("avro.schema");
        List<String> res = getFieldsList(url);
        HBaseTableSchema schema = new HBaseTableSchema();
        List<String> fields = new ArrayList<>();
        schema.setRowKey("rowkey", String.class);
        for (String field : res) {
            schema.addColumn("cf1", field.replaceAll("_", "").toUpperCase(), String.class);
            fields.add(field.replaceAll("_", "").toUpperCase());
        }
        return new Tuple2<HBaseTableSchema, List<String>>(schema, fields);
    }


    /**
     * 解析schema对象
     * 返回拼接hbase所需的list字段
     */
    private static List<String> getFieldsList(String url) {
        List<String> res = new ArrayList<>();
        String getSchema = HttpUtils.get(url);
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(getSchema);
        doFlat(schema, res);
        return res;
    }

    /**
     * 递归压平schema的层级field
     *
     * @param schema
     * @param res
     */
    private static void doFlat(Schema schema, List<String> res) {
        List<Schema.Field> fields = schema.getFields();
        fields.forEach(item -> {
            if (StringUtils.equals("RECORD", item.schema().getType().toString())) {
                doFlat(item.schema(), res);
            } else {
                res.add(item.name());
            }
        });
    }
}
