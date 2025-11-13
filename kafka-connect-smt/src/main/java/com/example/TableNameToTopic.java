package com.example;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;
import java.util.logging.Logger;


public class TableNameToTopic <R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger LOGGER = Logger.getLogger(Filter.class.getName());
    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public R apply(R record) {
        if (record.value() == null) {
            return record;
        }

        try {
            // extract table name from record Debezium
            Struct value = (Struct) record.value();
            Struct source = value.getStruct("source");
            String tableName = source.getString("table");

            LOGGER.info("TableName: " + tableName);

            if (tableName == null || tableName.isEmpty()) {
                return record;
            }

             return record.newRecord(
                tableName.toLowerCase(),  // Topic name mới
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                record.value(),
                record.timestamp()
            );

        } catch (Exception e) {
            return record;
        }
    }

    @Override
    public void close() {
        // Cleanup
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }
}