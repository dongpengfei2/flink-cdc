package com.dpf.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ChangelogLookup {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        tableEnvironment.executeSql("" +
            "CREATE TABLE dwd_binlog ( " +
            "op STRING," +
            "  data ROW(" +
            "  user_id INT, " +
            "  user_name STRING, " +
            "  mobile STRING, " +
            "  password STRING, " +
            "  create_time STRING " +
            "   )" +
            ") WITH ( " +
            "  'connector' = 'kafka', " +
            "  'topic' = 'dwd_binlog', " +
            "  'properties.bootstrap.servers' = '127.0.0.1:9092', " +
            "  'properties.enable.auto.commit' = 'false', " +
            "  'properties.session.timeout.ms' = '90000', " +
            "  'properties.request.timeout.ms' = '325000', " +
            "  'scan.startup.mode' = 'earliest-offset' , " +
            "  'value.format' = 'json' " +
            ")" +
            "");

        tableEnvironment.executeSql("select op, data.* from dwd_binlog where op='+I'").print();
    }
}
