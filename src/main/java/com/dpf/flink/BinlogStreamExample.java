package com.dpf.flink;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import java.util.List;
import java.util.Properties;

public class BinlogStreamExample {
    public static void main(String[] args) throws Exception {
        Properties extralPro = new Properties();
        extralPro.setProperty("AllowPublicKeyRetrieval", "true");
        SourceFunction<String> sourceFunction = MySQLSource.<SourceRecord>builder()
            .hostname("localhost")
            .port(3306)
            .databaseList("renren_fast") // monitor all tables under inventory database
            .username("root")
            .password("12345678")
            .debeziumProperties(extralPro)
            .deserializer(new JsonDebeziumDeserializationSchema())
            .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().registerKryoType(SourceRecord.class);
        env.setParallelism(1);
        final DataStreamSource<String> source = env.addSource(sourceFunction);
        source.print();

        env.execute();
    }

    public static String extractBeforeData(Struct value, Schema schema) {
        final Struct before = value.getStruct("before");
        final List<Field> fields = before.schema().fields();
        JSONObject jsonObject = new JSONObject();
        for (Field field : fields) {
            jsonObject.put(field.name(), before.get(field));
        }
        return jsonObject.toJSONString();
    }

    public static String extractAfterData(Struct value, Schema schema) {
        final Struct after = value.getStruct("after");
        final List<Field> fields = after.schema().fields();
        JSONObject jsonObject = new JSONObject();
        for (Field field : fields) {
            jsonObject.put(field.name(), after.get(field));
        }
        return jsonObject.toJSONString();
    }

    public static class JsonDebeziumDeserializationSchema implements DebeziumDeserializationSchema {

        @Override
        public TypeInformation getProducedType() {
            return TypeInformation.of(String.class);
        }

        @Override
        public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {
            final Operation op = Envelope.operationFor(sourceRecord);
            final String source = sourceRecord.topic();
            Struct value = (Struct) sourceRecord.value();
            final Schema schema = sourceRecord.valueSchema();
            if (op != Operation.CREATE && op != Operation.READ) {
                if (op == Operation.DELETE) {
                    String data = extractBeforeData(value, schema);
                    String record = new JSONObject()
                        .fluentPut("source", source)
                        .fluentPut("data", data)
                        .fluentPut("op", RowKind.DELETE.shortString())
                        .toJSONString();
                    collector.collect(record);

                } else {
                    String beforeData = extractBeforeData(value, schema);
                    String beforeRecord = new JSONObject()
                        .fluentPut("source", source)
                        .fluentPut("data", beforeData)
                        .fluentPut("op", RowKind.UPDATE_BEFORE.shortString())
                        .toJSONString();
                    collector.collect(beforeRecord);
                    String afterData = extractAfterData(value, schema);
                    String afterRecord = new JSONObject()
                        .fluentPut("source", source)
                        .fluentPut("data", afterData)
                        .fluentPut("op", RowKind.UPDATE_AFTER.shortString())
                        .toJSONString();
                    collector.collect(afterRecord);

                }
            } else {
                String data = extractAfterData(value, schema);

                String record = new JSONObject()
                    .fluentPut("source", source)
                    .fluentPut("data", data)
                    .fluentPut("op", RowKind.INSERT.shortString())
                    .toJSONString();
                collector.collect(record);
            }
        }
    }
}
