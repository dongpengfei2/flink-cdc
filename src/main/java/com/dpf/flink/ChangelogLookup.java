package com.dpf.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 该方法的运行需要修改源码，添加format的源数据
 */
public class ChangelogLookup {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        tableEnvironment.executeSql("" +
            "CREATE TABLE dwd_binlog ( " +
            "  user_id INT, " +
            "  username STRING, " +
            "  mobile STRING, " +
            "  password STRING, " +
            "  create_time STRING " +
            ") WITH ( " +
            "  'connector' = 'kafka', " +
            "  'topic' = 'dwd_binlog', " +
            "  'properties.bootstrap.servers' = '127.0.0.1:9092', " +
            "  'properties.enable.auto.commit' = 'false', " +
            "  'properties.session.timeout.ms' = '90000', " +
            "  'properties.request.timeout.ms' = '325000', " +
            "  'scan.startup.mode' = 'earliest-offset' , " +
            "  'value.format' = 'changelog-json' " +
            ")" +
            "");

        tableEnvironment.executeSql("select * from dwd_binlog").print();
    }
}
