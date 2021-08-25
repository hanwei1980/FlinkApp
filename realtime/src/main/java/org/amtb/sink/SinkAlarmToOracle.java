package org.amtb.sink;

import lombok.extern.slf4j.Slf4j;
import org.amtb.common.PropertyUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

/**
 * 实时报警记录存入oracle表
 *
 * @author hanwei
 * @version 1.0
 * @date 2021-01-25
 */
@Slf4j
public class SinkAlarmToOracle extends RichSinkFunction<String> {
    private Connection conn;
    private PreparedStatement ps;

    //构建连接
    @Override
    public void open(Configuration parameters) {
        try {
            Class.forName("oracle.jdbc.OracleDriver");
            conn = DriverManager.getConnection(PropertyUtil.get("jdbc.url"), PropertyUtil.get("jdbc.user"), PropertyUtil.get("jdbc.password"));
            ps = conn.prepareStatement("sql insert...");
        } catch (Exception e) {
            log.error("SinkAlarmToOracle->open() error:", e);
        }

    }

    //执行
    @Override
    public void invoke(String alarmRecord, Context context) {
        //给statement对象赋值 写入数据库
        try {
            ps.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
            ps.setString(2, alarmRecord);
            ps.execute();
        } catch (Exception e) {
            log.error("SinkAlarmToOracle sink输出异常: error-->", e);
        }
    }

    //关闭连接
    @Override
    public void close() {
        try {
            super.close();
            if (ps != null) {
                ps.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
            log.error("SinkAlarmToOracle->close() error:", e);
        }
    }
}
