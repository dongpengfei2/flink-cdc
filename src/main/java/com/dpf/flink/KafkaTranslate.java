package com.dpf.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KafkaTranslate {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        tableEnvironment.executeSql("" +
            "CREATE TABLE ods_binlog ( " +
            "  binlog_type STRING METADATA FROM 'value.binlog-type' VIRTUAL, " +
            "  binlog_es TIMESTAMP_LTZ(3) METADATA FROM 'value.event-timestamp' VIRTUAL, " +
            "  procTime AS PROCTIME(), " +
            "  user_id INT, " +
            "  username STRING, " +
            "  mobile STRING, " +
            "  password STRING, " +
            "  create_time STRING " +
            ") WITH ( " +
            "  'connector' = 'kafka', " +
            "  'topic' = 'ods_binlog', " +
            "  'properties.bootstrap.servers' = '127.0.0.1:9092', " +
            "  'properties.enable.auto.commit' = 'false', " +
            "  'properties.session.timeout.ms' = '90000', " +
            "  'properties.request.timeout.ms' = '325000', " +
            "  'value.format' = 'canal-json' " +
            ")" +
            "");

        tableEnvironment.executeSql("select * from ods_binlog where binlog_type='INSERT'").print();
    }
}
