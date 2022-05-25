package com.xiaopeng;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.ArrayList;

@Slf4j
public class WriteMysqlSink extends RichSinkFunction<String> implements SinkFunction<String> {
    private Connection connection = null;
    private PreparedStatement ps = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        log.info("获取数据库连接");
        connection = DbUtils.getConnection();
        String replaceSQL = "replace into table_name(a, b, c) values (?, ?, ?);";
        ps = connection.prepareStatement(replaceSQL);
    }

    @Override
    public void invoke(String energyJson, Context ctx) throws Exception {
        // 获取ReadMysqlResoure发送过来的结果
        if (energyJson == null){
            log.info("无法获取数据");
            return ;
        }

        ArrayList<ArrayList<Object>> res = IotJsonParsing.parsing(energyJson);
        for (ArrayList<Object> r: res) {
            ps.setString(1, (String) r.get(0));
            ps.setString(2, (String) r.get(1));
            ps.setTimestamp(3, (Timestamp) r.get(2));
            ps.addBatch();
        }

        // 一次性写入
        int[] count = ps.executeBatch();
        log.info("成功写入Mysql数量：" + count.length);

    }

    @Override
    public void close() throws Exception {
        //关闭并释放资源
        if(connection != null) {
            connection.close();
        }

        if(ps != null) {
            ps.close();
        }
    }

}
