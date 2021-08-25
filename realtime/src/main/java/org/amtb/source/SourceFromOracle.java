package org.amtb.source;

import lombok.extern.slf4j.Slf4j;
import org.amtb.common.PropertyUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * common source类，用于从oracle获取数据放入流中，可以是普通流也可以是广播流
 *
 * @author hanwei
 * @version 1.0
 * @date 2021-08-24 09:52
 */
@Slf4j
public class SourceFromOracle extends RichSourceFunction<HashMap<String, Object>> {
    private Connection connection;
    private PreparedStatement ps;
    private String driver = PropertyUtil.get("jdbc.driver");
    private String url = PropertyUtil.get("jdbc.url");
    private String username = PropertyUtil.get("jdbc.user");
    private String password = PropertyUtil.get("jdbc.password");
    private String sql = "select * from dual";
    private String typeName;
    /**
     * 解析规则查询周期为1分钟
     */
    private Long duration = StringUtils.isNotBlank(PropertyUtil.get("duration")) ? Long.parseLong(PropertyUtil.get("duration")) : 1L;

    private volatile boolean isRunning = true;

    public SourceFromOracle(String sql) {
        this.sql = sql;
    }

    public SourceFromOracle(String url, String username, String password, String sql) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.sql = sql;
        this.typeName = typeName;
    }

    @Override
    public void open(Configuration parameters) {
        try {
            super.open(parameters);
            Class.forName(driver);
            connection = DriverManager.getConnection(url, username, password);
            ps = connection.prepareStatement(sql);
        } catch (Exception e) {
            log.error(typeName + " --> SourceFromOracle -> open() error:", e);
        }
    }

    @Override
    public void run(SourceContext<HashMap<String, Object>> ctx) throws Exception {
        while (isRunning) {
            try {
                ResultSet resultSet = ps.executeQuery();
                while (resultSet.next()) {
                    HashMap<String, Object> resuteItem = new HashMap<>();
                    ResultSetMetaData rsm = resultSet.getMetaData();
                    for (int i = 1; i <= rsm.getColumnCount(); i++) {
                        resuteItem.put(rsm.getColumnLabel(i), resultSet.getObject(i) != null ? resultSet.getObject(i) : "");
                    }
                    ctx.collect(resuteItem);
                }
            } catch (Exception e) {
                log.error(typeName + " --> SourceFromOracle -> run() error:", e);
            }
            //线程睡眠
            TimeUnit.MINUTES.sleep(duration);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    // 关闭
    @Override
    public void close() {
        try {
            super.close();
            if (ps != null) {
                ps.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            log.error(typeName + " --> SourceFromOracle -> close() error:", e);
        }
    }
}
