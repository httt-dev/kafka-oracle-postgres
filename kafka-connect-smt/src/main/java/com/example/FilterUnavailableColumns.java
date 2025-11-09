package com.example;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.*;
import java.util.stream.Collectors;

public class FilterUnavailableColumns<R extends ConnectRecord<R>> implements Transformation<R> ,Versioned {

    private static final String PLACEHOLDER_VALUES_CONFIG = "placeholder.values";
    private static final String DEFAULT_PLACEHOLDERS = "__ORACLE_LOB_UNAVAILABLE__,__debezium_unavailable_value";

    private Set<String> placeholders;

    @Override
    public void configure(Map<String, ?> configs) {
        Object raw = configs.get(PLACEHOLDER_VALUES_CONFIG);
        String cfg = raw == null ? DEFAULT_PLACEHOLDERS : String.valueOf(raw);

        if (cfg == null || cfg.trim().isEmpty()) {
            throw new ConfigException(PLACEHOLDER_VALUES_CONFIG, cfg, "Must provide at least one placeholder value.");
        }
        placeholders = Arrays.stream(cfg.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());
    }

    @Override
    public R apply(R record) {
        Object value = record.value();
        if (value == null) {
            return record;
        }

        if (value instanceof Struct) {
            Struct struct = (Struct) value;
            Schema schema = struct.schema();

            // Envelope của Debezium có field "after" và "before"
            Field afterField = schema.field("after");
            Field beforeField = schema.field("before");

            if (afterField != null || beforeField != null) {
                Struct newAfter = afterField != null ? filterStruct((Struct) struct.get(afterField)) : null;
                Struct newBefore = beforeField != null ? filterStruct((Struct) struct.get(beforeField)) : null;

                // Tạo schema envelope mới, nhưng giữ nguyên các field khác
                SchemaBuilder envelopeBuilder = SchemaBuilder.struct().name(schema.name());
                for (Field f : schema.fields()) {
                    if ("after".equals(f.name()) && newAfter != null) {
                        envelopeBuilder.field("after", newAfter.schema());
                    } else if ("before".equals(f.name()) && newBefore != null) {
                        envelopeBuilder.field("before", newBefore.schema());
                    } else {
                        envelopeBuilder.field(f.name(), f.schema());
                    }
                }
                Schema newEnvelopeSchema = envelopeBuilder.build();

                Struct newEnvelope = new Struct(newEnvelopeSchema);
                for (Field f : newEnvelopeSchema.fields()) {
                    if ("after".equals(f.name())) {
                        newEnvelope.put("after", newAfter);
                    } else if ("before".equals(f.name())) {
                        newEnvelope.put("before", newBefore);
                    } else {
                        newEnvelope.put(f.name(), struct.get(f.name()));
                    }
                }

                return record.newRecord(
                        record.topic(),
                        record.kafkaPartition(),
                        record.keySchema(),
                        record.key(),
                        newEnvelopeSchema,
                        newEnvelope,
                        record.timestamp()
                );
            } else {
                // Không phải envelope: xử lý struct đơn (ví dụ sau khi Unwrap)
                Struct filtered = filterStruct(struct);
                return record.newRecord(
                        record.topic(),
                        record.kafkaPartition(),
                        record.keySchema(),
                        record.key(),
                        filtered.schema(),
                        filtered,
                        record.timestamp()
                );
            }
        }

        // Không hỗ trợ kiểu Map/primitive ở đây
        return record;
    }

    private Struct filterStruct(Struct struct) {
        if (struct == null) return null;

        Schema originalSchema = struct.schema();
        SchemaBuilder filteredSchemaBuilder = SchemaBuilder.struct().name(originalSchema.name());
        for (Field field : originalSchema.fields()) {
            Object value = struct.get(field);
            if (value instanceof String && placeholders.contains(value)) {
                // Bỏ field có giá trị placeholder
                continue;
            }
            filteredSchemaBuilder.field(field.name(), field.schema());
        }
        Schema filteredSchema = filteredSchemaBuilder.build();

        Struct filteredStruct = new Struct(filteredSchema);
        for (Field field : filteredSchema.fields()) {
            filteredStruct.put(field.name(), struct.get(field.name()));
        }
        return filteredStruct;
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(PLACEHOLDER_VALUES_CONFIG, ConfigDef.Type.STRING, DEFAULT_PLACEHOLDERS, ConfigDef.Importance.MEDIUM,
                        "Comma-separated placeholder values that indicate unavailable LOBs.");
    }

    @Override
    public void close() {
        // nothing to close
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}