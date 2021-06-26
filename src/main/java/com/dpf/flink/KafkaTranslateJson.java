package com.dpf.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 读取kafka中canal json的数据，解析之后以json的方式存入kafka dwd层
 */
public class KafkaTranslateJson {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        tableEnvironment.executeSql("" +
            "CREATE TABLE ods_binlog ( " +
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

        tableEnvironment.executeSql("" +
            "CREATE TABLE kafka_binlog ( " +
            "  user_id INT, " +
            "  user_name STRING, " +
            "  mobile STRING, " +
            "  password STRING, " +
            "  create_time STRING, " +
            "  PRIMARY KEY (user_id) NOT ENFORCED" +
            ") WITH ( " +
            "  'connector' = 'upsert-kafka', " +
            "  'topic' = 'mysql_binlog', " +
            "  'properties.bootstrap.servers' = '127.0.0.1:9092', " +
            "  'key.format' = 'json', " +
            "  'value.format' = 'json' " +
            ")" +
            "");

        tableEnvironment.executeSql("insert into kafka_binlog select * from ods_binlog");
    }
}