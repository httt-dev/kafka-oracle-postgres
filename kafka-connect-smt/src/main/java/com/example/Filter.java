package com.example;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.apache.kafka.connect.data.Struct;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class Filter<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final Logger LOGGER = Logger.getLogger(Filter.class.getName());
    private static final String CONDITION_FIELD_CONFIG = "field";

    public static final String OVERVIEW_DOC =
            "Filter records based on a dynamic condition passed via configuration.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(CONDITION_FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                    "The JavaScript condition to filter records.");

    private String conditionField;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        this.conditionField = config.getString(CONDITION_FIELD_CONFIG);
        if (conditionField == null || conditionField.isEmpty()) {
            throw new ConfigException(CONDITION_FIELD_CONFIG, conditionField, "Condition field cannot be null or empty.");
        }
        System.out.println("Configuration completed with conditionField: " + conditionField);
    }

    @Override
    public R apply(R record) {
        if (record == null || record.value() == null) {
            LOGGER.warning("Record is null or value is null.");
            return null; // Drop the record if it's null or the value is null
        }

        if (record.value() instanceof Map) {
            Map<String, Object> payload = (Map<String, Object>) record.value();
            boolean conditionMet = evaluateCondition(payload, conditionField);

            if (conditionMet) {
                LOGGER.info("Record passed the filter criteria.");
                System.out.println("Record passed the filter criteria.");
                return record;
            } else {
                LOGGER.info("Record filtered out based on the condition.");
                System.out.println("Record filtered out based on the condition.");
                return null;
            }
        } else if (record.value() instanceof Struct) {
            Struct struct = (Struct) record.value();
            System.out.println("Record Struct value: " + struct.toString());

            // Here you can add your logic to evaluate the condition for Struct type if needed
            boolean conditionMet = evaluateConditionForStruct(struct, conditionField);

            if (conditionMet) {
                LOGGER.info("Record passed the filter criteria.");
                System.out.println("Record passed the filter criteria.");
                return record;
            } else {
                LOGGER.info("Record filtered out based on the condition.");
                System.out.println("Record filtered out based on the condition.");
                return null;
            }
        } else {
            LOGGER.warning("Record value is of unexpected type: " + record.value().getClass().getName());
            System.out.println("Record value is of unexpected type: " + record.value().getClass().getName());
            return null;
        }
    }

    private boolean evaluateCondition(Map<String, Object> payload, String condition) {

        try {

            ScriptEngine engine = new ScriptEngineManager().getEngineByName("JavaScript");
            if (engine == null) {
                LOGGER.severe("JavaScript ScriptEngine is not available.");

                return false;
            }
            engine.put("payload", payload);
            Object result = engine.eval(condition);

            if (result instanceof Boolean) {
                return (Boolean) result;
            } else {
                LOGGER.severe("Condition did not return a boolean result.");

                return false;
            }
        } catch (ScriptException e) {
            LOGGER.severe("Error evaluating condition: " + e.getMessage());
            return false;
        }
    }

    private boolean evaluateConditionForStruct(Struct struct, String condition) {
    try {

        // Convert the Struct to a Map to make it compatible with JavaScript evaluation
        Map<String, Object> payload = struct.schema().fields().stream()
                .filter(field -> struct.get(field) != null) // Filter out fields with null values
                .collect(Collectors.toMap(
                    field -> field.name(),
                    field -> struct.get(field)
                ));

        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("JavaScript");
        engine.put("payload", payload);
        Object result = engine.eval(condition);

        if (result instanceof Boolean) {
            return (Boolean) result;
        } else {
            LOGGER.severe("Condition did not return a boolean result.");
            System.out.println("Condition did not return a boolean result.");
            return false;
        }
        } catch (ScriptException e) {
        LOGGER.severe("Error evaluating condition: " + e.getMessage());
        return false;
        } catch (NullPointerException e) {
        LOGGER.severe("Null value encountered during evaluation: " + e.getMessage());
        return false;
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        // No resources to clean up
        System.out.println("Closing resources.");
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}