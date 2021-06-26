# Flink CDC

主要用于扩展测试Flink CDC的功能，为了实现根据canal type的过滤，修改了一点源码

1. 修改org.apache.flink.formats.json.canal.CanalJsonDecodingFormat
```
static enum ReadableMetadata {
        DATABASE("database", (DataType)DataTypes.STRING().nullable(), DataTypes.FIELD("database", DataTypes.STRING()), new MetadataConverter() {
            private static final long serialVersionUID = 1L;

            public Object convert(GenericRowData row, int pos) {
                return row.getString(pos);
            }
        }),
        TABLE("table", (DataType)DataTypes.STRING().nullable(), DataTypes.FIELD("table", DataTypes.STRING()), new MetadataConverter() {
            private static final long serialVersionUID = 1L;

            public Object convert(GenericRowData row, int pos) {
                return row.getString(pos);
            }
        }),
        //todo 修改元数据，添加binlog-type
        BINLOG_TYPE("binlog-type", (DataType)DataTypes.STRING().nullable(), DataTypes.FIELD("type", DataTypes.STRING()), new MetadataConverter() {
            private static final long serialVersionUID = 1L;

            public Object convert(GenericRowData row, int pos) {
                return row.getString(pos);
            }
        }),
        SQL_TYPE("sql-type", (DataType)DataTypes.MAP((DataType)DataTypes.STRING().nullable(), (DataType)DataTypes.INT().nullable()).nullable(), DataTypes.FIELD("sqlType", DataTypes.MAP((DataType)DataTypes.STRING().nullable(), (DataType)DataTypes.INT().nullable())), new MetadataConverter() {
            private static final long serialVersionUID = 1L;

            public Object convert(GenericRowData row, int pos) {
                return row.getMap(pos);
            }
        }),
        PK_NAMES("pk-names", (DataType)DataTypes.ARRAY(DataTypes.STRING()).nullable(), DataTypes.FIELD("pkNames", DataTypes.ARRAY(DataTypes.STRING())), new MetadataConverter() {
            private static final long serialVersionUID = 1L;

            public Object convert(GenericRowData row, int pos) {
                return row.getArray(pos);
            }
        }),
        INGESTION_TIMESTAMP("ingestion-timestamp", (DataType)DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).nullable(), DataTypes.FIELD("ts", DataTypes.BIGINT()), new MetadataConverter() {
            private static final long serialVersionUID = 1L;

            public Object convert(GenericRowData row, int pos) {
                return row.isNullAt(pos) ? null : TimestampData.fromEpochMillis(row.getLong(pos));
            }
        }),
        EVENT_TIMESTAMP("event-timestamp", (DataType)DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).nullable(), DataTypes.FIELD("es", DataTypes.BIGINT()), new MetadataConverter() {
            private static final long serialVersionUID = 1L;

            public Object convert(GenericRowData row, int pos) {
                return row.isNullAt(pos) ? null : TimestampData.fromEpochMillis(row.getLong(pos));
            }
        });

        final String key;
        final DataType dataType;
        final Field requiredJsonField;
        final MetadataConverter converter;

        private ReadableMetadata(String key, DataType dataType, Field requiredJsonField, MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.requiredJsonField = requiredJsonField;
            this.converter = converter;
        }
    }
```
否则会报错
```
Exception in thread "main" org.apache.flink.table.api.ValidationException: Invalid metadata key 'value.binlog-type' in column 'binlog_type' of table 'default_catalog.default_database.ods_binlog'. The DynamicTableSource class 'org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSource' supports the following metadata keys for reading:
value.database
value.table
value.sql-type
value.pk-names
value.ingestion-timestamp
value.event-timestamp
topic
partition
headers
leader-epoch
offset
timestamp
timestamp-type
	at org.apache.flink.table.planner.connectors.DynamicSourceUtils.lambda$validateAndApplyMetadata$6(DynamicSourceUtils.java:403)
	at java.util.ArrayList.forEach(ArrayList.java:1257)
	at org.apache.flink.table.planner.connectors.DynamicSourceUtils.validateAndApplyMetadata(DynamicSourceUtils.java:395)
	at org.apache.flink.table.planner.connectors.DynamicSourceUtils.prepareDynamicSource(DynamicSourceUtils.java:158)
	at org.apache.flink.table.planner.connectors.DynamicSourceUtils.convertSourceToRel(DynamicSourceUtils.java:119)
	at org.apache.flink.table.planner.plan.schema.CatalogSourceTable.toRel(CatalogSourceTable.java:85)
```
2. 修改org.apache.flink.formats.json.canal.CanalJsonDeserializationSchema
```
    private static RowType createJsonRowType(
            DataType physicalDataType, List<ReadableMetadata> readableMetadata) {
        // Canal JSON contains other information, e.g. "ts", "sql", but we don't need them
        DataType root =
                DataTypes.ROW(
                        DataTypes.FIELD("data", DataTypes.ARRAY(physicalDataType)),
                        DataTypes.FIELD("old", DataTypes.ARRAY(physicalDataType)),
                        DataTypes.FIELD("type", DataTypes.STRING()),
                        ReadableMetadata.DATABASE.requiredJsonField,
                        ReadableMetadata.TABLE.requiredJsonField);
        // append fields that are required for reading metadata in the root
        final List<DataTypes.Field> rootMetadataFields =
                readableMetadata.stream()
                        .filter(m -> m != ReadableMetadata.DATABASE && m != ReadableMetadata.TABLE && m != ReadableMetadata.BINLOG_TYPE)
                        .map(m -> m.requiredJsonField)
                        .distinct()
                        .collect(Collectors.toList());
        return (RowType) DataTypeUtils.appendRowFields(root, rootMetadataFields).getLogicalType();
    }
```
否则会报错
```
Exception in thread "main" org.apache.flink.table.api.ValidationException: Field names must be unique. Found duplicates: [type]
	at org.apache.flink.table.types.logical.RowType.validateFields(RowType.java:272)
	at org.apache.flink.table.types.logical.RowType.<init>(RowType.java:157)
	at org.apache.flink.table.types.utils.DataTypeUtils.appendRowFields(DataTypeUtils.java:181)
	at org.apache.flink.formats.json.canal.CanalJsonDeserializationSchema.createJsonRowType(CanalJsonDeserializationSchema.java:370)
	at org.apache.flink.formats.json.canal.CanalJsonDeserializationSchema.<init>(CanalJsonDeserializationSchema.java:111)
	at org.apache.flink.formats.json.canal.CanalJsonDeserializationSchema.<init>(CanalJsonDeserializationSchema.java:61)
	at org.apache.flink.formats.json.canal.CanalJsonDeserializationSchema$Builder.build(CanalJsonDeserializationSchema.java:188)
	at org.apache.flink.formats.json.canal.CanalJsonDecodingFormat.createRuntimeDecoder(CanalJsonDecodingFormat.java:104)
	at org.apache.flink.formats.json.canal.CanalJsonDecodingFormat.createRuntimeDecoder(CanalJsonDecodingFormat.java:46)
	at org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSource.createDeserialization(KafkaDynamicSource.java:427)
	at org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSource.getScanRuntimeProvider(KafkaDynamicSource.java:199)
	at org.apache.flink.table.planner.connectors.DynamicSourceUtils.validateScanSource(DynamicSourceUtils.java:453)
	at org.apache.flink.table.planner.connectors.DynamicSourceUtils.prepareDynamicSource(DynamicSourceUtils.java:161)
	at org.apache.flink.table.planner.connectors.DynamicSourceUtils.convertSourceToRel(DynamicSourceUtils.java:119)
	at org.apache.flink.table.planner.plan.schema.CatalogSourceTable.toRel(CatalogSourceTable.java:85)
```