package org.amtb.sink;

import lombok.extern.slf4j.Slf4j;
import org.amtb.common.PropertyUtil;
import org.amtb.entity.RealTime;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

/**
 * oracle common sink
 *
 * @author hanwei
 * @version 1.0
 * @date 2021-08-24 09:41
 */
@Slf4j
public class SinkOracle extends RichSinkFunction<RealTime> {
    private Connection conn;
    private PreparedStatement ps;

    // 1,初始化
    @Override
    public void open(Configuration parameters) {
        try {
            super.open(parameters);
            Class.forName("oracle.jdbc.OracleDriver");
            conn = DriverManager.getConnection(PropertyUtil.get("jdbc.url"), PropertyUtil.get("jdbc.user"),
                    PropertyUtil.get("jdbc.password"));
            ps = conn.prepareStatement(
                    "INSERT INTO REALTIME (UUID,DEVICE_CODE,MACHINE_STATE,ACTIVE_ENERGY,PRODUCTION,CREATETIME) VALUES (?,?,?,?,?,?)");
        } catch (Exception e) {
            log.error("SinkOracle->open() error:", e);
        }

    }

    // 2,执行
    @Override
    public void invoke(RealTime realTime, Context context) {
        try {
            ps.setString(1, realTime.getUuid());
            ps.setString(2, realTime.getDeviceCode());
            ps.setString(3, realTime.getMachineState());
            ps.setDouble(4, realTime.getActiveEnergy());
            ps.setInt(5, realTime.getProduction());
            ps.setTimestamp(6, new Timestamp(realTime.getCreateTime()));
            ps.execute();
        } catch (Exception e) {
                log.error("SinkOracle->invoke() error:", e);
        }
    }

    // 3,关闭
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
            log.error("SinkOracle->close() error:", e);
        }
    }
}
