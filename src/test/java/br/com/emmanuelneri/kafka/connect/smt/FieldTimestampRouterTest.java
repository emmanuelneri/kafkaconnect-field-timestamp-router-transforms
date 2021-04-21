package br.com.emmanuelneri.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

class FieldTimestampRouterTest {

    private static final String TOPIC = "test";
    private static final String FIELD_NAME_CONFIG = "field.name";

    @Test
    public void shouldFailWhenConfigureRouteWithoutFieldNameProperty() {
        final FieldTimestampRouter<SourceRecord> fieldTimestampRouter = new FieldTimestampRouter<>();
        Assertions.assertThrows(ConfigException.class, () -> fieldTimestampRouter.configure(Collections.emptyMap()));
    }

    @Test
    public void shouldFailWhenApplyRouterToRecordWithoutField() {
        final FieldTimestampRouter<SourceRecord> fieldTimestampRouter = new FieldTimestampRouter<>();
        fieldTimestampRouter.configure(createConfig("name"));

        Map<String, Object> record = new HashMap<>();
        record.put("id", UUID.randomUUID().toString());

        final SourceRecord sourceRecord = new SourceRecord(
                null, null,
                TOPIC, 0,
                Schema.STRING_SCHEMA, UUID.randomUUID().toString(),
                null, record,
                Timestamp.from(Instant.now()).getTime()
        );

        Assertions.assertThrows(DataException.class,
                () -> fieldTimestampRouter.apply(sourceRecord).topic());
    }

    @Test
    public void shouldFormatTopicNameWithNameAndFieldValueAndDateWhenApplyAValidRecord() {
        final FieldTimestampRouter<SourceRecord> fieldTimestampRouter = new FieldTimestampRouter<>();
        fieldTimestampRouter.configure(createConfig("type"));

        Map<String, Object> record = new HashMap<>();
        record.put("id", UUID.randomUUID().toString());
        record.put("type", "confirmed");

        final SourceRecord sourceRecord = new SourceRecord(
                null, null,
                TOPIC, 0,
                Schema.STRING_SCHEMA, UUID.randomUUID().toString(),
                null, record,
                Timestamp.valueOf(LocalDateTime.of(2020, 4, 21, 13, 0)).getTime()
        );

        Assertions.assertEquals("test-confirmed-20200421", fieldTimestampRouter.apply(sourceRecord).topic());
    }

    @Test
    public void shouldFormatTopicNameWithNameAndFieldValueAndDateWhenApplyARecordWithNumberValue() {
        final FieldTimestampRouter<SourceRecord> fieldTimestampRouter = new FieldTimestampRouter<>();
        final Map<String, Object> config = createConfig("statusNumber");
        fieldTimestampRouter.configure(config);

        Map<String, Object> record = new HashMap<>();
        record.put("id", UUID.randomUUID().toString());
        record.put("statusNumber", 10);

        final SourceRecord sourceRecord = new SourceRecord(
                null, null,
                TOPIC, 0,
                Schema.STRING_SCHEMA, UUID.randomUUID().toString(),
                null, record,
                Timestamp.valueOf(LocalDateTime.of(2020, 4, 21, 13, 0)).getTime()
        );

        Assertions.assertEquals("test-10-20200421", fieldTimestampRouter.apply(sourceRecord).topic());
    }

    @Test
    public void shouldFailWhenApplyARecordWithUnexpectedTypeValueInConfiguredField() {
        final FieldTimestampRouter<SourceRecord> fieldTimestampRouter = new FieldTimestampRouter<>();
        final Map<String, Object> config = createConfig("values");
        fieldTimestampRouter.configure(config);

        Map<String, Object> record = new HashMap<>();
        record.put("id", UUID.randomUUID().toString());
        record.put("values", Arrays.asList("AAA", "BBB"));

        final SourceRecord sourceRecord = new SourceRecord(
                null, null,
                TOPIC, 0,
                Schema.STRING_SCHEMA, UUID.randomUUID().toString(),
                null, record,
                Timestamp.valueOf(LocalDateTime.of(2020, 4, 21, 13, 0)).getTime()
        );

        Assertions.assertThrows(DataException.class,
                () -> fieldTimestampRouter.apply(sourceRecord).topic());
    }

    @Test
    public void shouldCustomDateFormatTopicNameWithNameAndFieldValueAndDateWhenApplyAValidRecord() {
        final FieldTimestampRouter<SourceRecord> fieldTimestampRouter = new FieldTimestampRouter<>();
        final Map<String, Object> config = createConfig("type");
        config.put("timestamp.format", "YYYYMM");
        fieldTimestampRouter.configure(config);

        Map<String, Object> record = new HashMap<>();
        record.put("id", UUID.randomUUID().toString());
        record.put("type", "confirmed");

        final SourceRecord sourceRecord = new SourceRecord(
                null, null,
                TOPIC, 0,
                Schema.STRING_SCHEMA, UUID.randomUUID().toString(),
                null, record,
                Timestamp.valueOf(LocalDateTime.of(2020, 4, 21, 13, 0)).getTime()
        );

        Assertions.assertEquals("test-confirmed-202004", fieldTimestampRouter.apply(sourceRecord).topic());
    }

    @Test
    public void shouldCustomTopicNameWithNameAndFieldValueAndDateWhenApplyAValidRecord() {
        final FieldTimestampRouter<SourceRecord> fieldTimestampRouter = new FieldTimestampRouter<>();
        final Map<String, Object> config = createConfig("type");
        config.put("topic.format", "${field}-${topic}-${timestamp}");
        fieldTimestampRouter.configure(config);

        Map<String, Object> record = new HashMap<>();
        record.put("id", UUID.randomUUID().toString());
        record.put("type", "confirmed");

        final SourceRecord sourceRecord = new SourceRecord(
                null, null,
                TOPIC, 0,
                Schema.STRING_SCHEMA, UUID.randomUUID().toString(),
                null, record,
                Timestamp.valueOf(LocalDateTime.of(2020, 4, 21, 13, 0)).getTime()
        );

        Assertions.assertEquals("confirmed-test-20200421", fieldTimestampRouter.apply(sourceRecord).topic());
    }

    @Test
    public void shouldCustomTopicNameWithOnlyFieldValueAndDateWhenApplyAValidRecord() {
        final FieldTimestampRouter<SourceRecord> fieldTimestampRouter = new FieldTimestampRouter<>();
        final Map<String, Object> config = createConfig("type");
        config.put("topic.format", "${field}-${timestamp}");
        fieldTimestampRouter.configure(config);

        Map<String, Object> record = new HashMap<>();
        record.put("id", UUID.randomUUID().toString());
        record.put("type", "confirmed");

        final SourceRecord sourceRecord = new SourceRecord(
                null, null,
                TOPIC, 0,
                Schema.STRING_SCHEMA, UUID.randomUUID().toString(),
                null, record,
                Timestamp.valueOf(LocalDateTime.of(2020, 4, 21, 13, 0)).getTime()
        );

        Assertions.assertEquals("confirmed-20200421", fieldTimestampRouter.apply(sourceRecord).topic());
    }

    private Map<String, Object> createConfig(final String fieldName) {
        final Map<String, Object> configuration = new HashMap<>();
        configuration.put(FIELD_NAME_CONFIG, fieldName);
        return configuration;
    }

}