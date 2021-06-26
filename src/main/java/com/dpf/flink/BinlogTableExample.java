package com.dpf.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class BinlogTableExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        tableEnvironment.executeSql("" +
            " CREATE TABLE mysql_binlog ( " +
            "  user_id INT, " +
            "  username STRING, " +
            "  mobile STRING, " +
            "  password STRING, " +
            "  create_time STRING " +
            " ) WITH ( " +
            "  'connector' = 'mysql-cdc', " +
            "  'hostname' = 'localhost', " +
            "  'port' = '3306', " +
            "  'username' = 'root', " +
            "  'password' = '12345678', " +
            "  'database-name' = 'renren_fast', " +
            "  'table-name' = 'tb_user' " +
            " )" +
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

        tableEnvironment.executeSql("insert into kafka_binlog select * from mysql_binlog");
    }
}