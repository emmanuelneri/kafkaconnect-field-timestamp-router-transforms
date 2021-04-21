package br.com.emmanuelneri.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;

public class FieldTimestampRouter<R extends ConnectRecord<R>> implements Transformation<R>, AutoCloseable {

    public static final String OVERVIEW_DOC = "Update the record's topic to a new topic format composed of topic name, field value and timestamp";

    private static final Pattern TOPIC = Pattern.compile("${topic}", Pattern.LITERAL);
    private static final Pattern TIMESTAMP = Pattern.compile("${timestamp}", Pattern.LITERAL);
    private static final Pattern FIELD = Pattern.compile("${field}", Pattern.LITERAL);
    private static final String FIELD_PURPOSE = "composite topic name";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.TOPIC_FORMAT, ConfigDef.Type.STRING, "${topic}-${field}-${timestamp}", ConfigDef.Importance.HIGH,
                    "Format string which can contain <code>${topic}</code>, <code>${field}</code> and <code>${timestamp}</code> as placeholders for the topic, field name and timestamp, respectively.")
            .define(ConfigName.TIMESTAMP_FORMAT, ConfigDef.Type.STRING, "yyyyMMdd", ConfigDef.Importance.HIGH,
                    "Format string for the timestamp that is compatible with <code>java.text.SimpleDateFormat</code>.")
            .define(ConfigName.FIELD_NAME, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH,
                    "Record field name to composite topic name.");

    public static final class ConfigName {
        private ConfigName() {
        }

        private static final String TOPIC_FORMAT = "topic.format";
        private static final String TIMESTAMP_FORMAT = "timestamp.format";
        private static final String FIELD_NAME = "field.name";
    }

    private String topicFormat;
    private String fieldName;
    private ThreadLocal<SimpleDateFormat> timestampFormat;

    @Override
    public void configure(final Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        this.topicFormat = config.getString(ConfigName.TOPIC_FORMAT);
        this.fieldName = config.getString(ConfigName.FIELD_NAME);

        final String timestampFormatStr = config.getString(ConfigName.TIMESTAMP_FORMAT);
        this.timestampFormat = ThreadLocal.withInitial(() -> {
            final SimpleDateFormat dateFormat = new SimpleDateFormat(timestampFormatStr);
            dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
            return dateFormat;
        });
    }

    @Override
    public R apply(R record) {
        final Long timestamp = record.timestamp();
        if (timestamp == null) {
            throw new DataException("Timestamp missing on record: " + record);
        }

        final Map<String, Object> recordFieldMap = requireMap(record.value(), FIELD_PURPOSE);
        final Object fieldValue = recordFieldMap.get(this.fieldName);
        if (fieldValue == null) {
            throw new DataException(String.format("Required field %s no exists on record: %s", this.fieldName, record.key()));
        }

        final String fieldValueAsString = formatFieldValue(fieldValue);
        final String newTopicName = buildNewTopicName(record.topic(), timestamp, fieldValueAsString);

        return record.newRecord(
                newTopicName, record.kafkaPartition(),
                record.keySchema(), record.key(),
                record.valueSchema(), record.value(),
                record.timestamp()
        );
    }

    private String buildNewTopicName(final String originTopicName, final Long timestamp, final String fieldValue) {
        final String formattedTimestamp = timestampFormat.get().format(new Date(timestamp));
        final String topicReplace = TOPIC.matcher(topicFormat).replaceAll(Matcher.quoteReplacement(originTopicName));
        final String fieldReplace = FIELD.matcher(topicReplace).replaceAll(Matcher.quoteReplacement(fieldValue));
        return TIMESTAMP.matcher(fieldReplace).replaceAll(Matcher.quoteReplacement(formattedTimestamp));
    }

    private String formatFieldValue(final Object value) {
        if (value instanceof String) {
            return (String) value;
        }

        if (value instanceof Number) {
            return value.toString();
        }

        throw new DataException("Invalid field type to value: " + value);
    }

    @Override
    public void close() {
        this.timestampFormat.remove();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
