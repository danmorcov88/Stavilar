package io.github.danmorcov88.stavilar.processors;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.data.ClickHouseFormat;
import io.github.danmorcov88.stavilar.api.ClickHouseConnectionService;
import io.github.danmorcov88.stavilar.api.ClickHouseSettings;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Tags({"clickhouse", "insert", "put", "record", "database"})
@CapabilityDescription("Inserts records into a ClickHouse table as JSONEachRow through the ClickHouse HTTP interface. "
        + "Large FlowFiles are sent in several inserts of at most 'Max Rows Per Insert' rows. "
        + "With a deduplication token, sending the same FlowFile again does not duplicate rows "
        + "on tables that keep a deduplication window.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@DynamicProperty(name = "ch.setting.<name>", value = "setting value",
        description = "A ClickHouse server setting sent with every insert made by this processor, "
                + "for example ch.setting.max_insert_block_size = 1048576. Overrides the same setting on the connection service.",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
@WritesAttributes({
        @WritesAttribute(attribute = PutClickHouseRecord.ATTR_ROWS_WRITTEN, description = "Rows accepted by ClickHouse, summed over all inserts. "
                + "An insert dropped by deduplication still reports its rows."),
        @WritesAttribute(attribute = PutClickHouseRecord.ATTR_INSERTS, description = "Number of insert requests made for the FlowFile"),
        @WritesAttribute(attribute = PutClickHouseRecord.ATTR_QUERY_ID, description = "ClickHouse query id of each insert, comma-separated"),
        @WritesAttribute(attribute = PutClickHouseRecord.ATTR_ERROR, description = "Error message when routed to failure or retry")})
public class PutClickHouseRecord extends AbstractProcessor {

    static final String ATTR_ROWS_WRITTEN = "clickhouse.rows.written";
    static final String ATTR_INSERTS = "clickhouse.inserts";
    static final String ATTR_QUERY_ID = "clickhouse.query.id";
    static final String ATTR_ERROR = "clickhouse.error";

    static final String TOKEN_NONE = "None";
    static final String TOKEN_FLOWFILE_UUID = "FlowFile UUID";
    static final String TOKEN_ATTRIBUTE = "Attribute";

    public static final PropertyDescriptor CONNECTION_SERVICE = new PropertyDescriptor.Builder()
            .name("ClickHouse Connection Service")
            .required(true)
            .identifiesControllerService(ClickHouseConnectionService.class)
            .build();

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("Reads the FlowFile content into records.")
            .required(true)
            .identifiesControllerService(RecordReaderFactory.class)
            .build();

    public static final PropertyDescriptor DATABASE = new PropertyDescriptor.Builder()
            .name("Database")
            .description("Database of the target table. Empty means the database of the connection service.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor TABLE = new PropertyDescriptor.Builder()
            .name("Table")
            .description("Target table, as written in SQL. Quote it with backticks if the name needs quoting.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor MAX_ROWS_PER_INSERT = new PropertyDescriptor.Builder()
            .name("Max Rows Per Insert")
            .description("A FlowFile with more records than this is sent in several inserts. Each insert is held in memory before it is sent.")
            .required(true)
            .defaultValue("100000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor DEDUPLICATION_TOKEN = new PropertyDescriptor.Builder()
            .name("Deduplication Token")
            .description("Value sent as insert_deduplication_token. On tables with a deduplication window "
                    + "(Replicated*MergeTree, or MergeTree with non_replicated_deduplication_window > 0) ClickHouse drops an insert "
                    + "whose token it has already seen, so retrying a FlowFile does not duplicate rows. "
                    + "When a FlowFile needs several inserts, insert i uses '<token>-<i>' (the first one uses the token as is).")
            .required(true)
            .allowableValues(TOKEN_NONE, TOKEN_FLOWFILE_UUID, TOKEN_ATTRIBUTE)
            .defaultValue(TOKEN_NONE)
            .build();

    public static final PropertyDescriptor DEDUPLICATION_TOKEN_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("Deduplication Token Attribute")
            .description("FlowFile attribute whose value is the deduplication token.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(DEDUPLICATION_TOKEN, TOKEN_ATTRIBUTE)
            .build();

    public static final PropertyDescriptor ASYNC_INSERT = new PropertyDescriptor.Builder()
            .name("Async Insert")
            .description("Sends async_insert=1 and wait_for_async_insert=1, so ClickHouse buffers small inserts server-side. "
                    + "Cannot be combined with a deduplication token: ClickHouse ignores the token on async inserts.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor WAIT_END_OF_QUERY = new PropertyDescriptor.Builder()
            .name("Wait End Of Query")
            .description("Sends wait_end_of_query=1, so the response comes only after the data is written, "
                    + "including into materialized views. Turn off only if you accept an earlier response.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All records were inserted.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("The records could not be read, or ClickHouse rejected the data (unknown column, bad value, missing table).")
            .build();

    public static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("A network problem or a temporary server condition (too many parts, memory limit) stopped the insert.")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            CONNECTION_SERVICE, RECORD_READER, DATABASE, TABLE, MAX_ROWS_PER_INSERT,
            DEDUPLICATION_TOKEN, DEDUPLICATION_TOKEN_ATTRIBUTE, ASYNC_INSERT, WAIT_END_OF_QUERY);

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE, REL_RETRY);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return ClickHouseSettings.dynamicProperty(propertyDescriptorName, ExpressionLanguageScope.FLOWFILE_ATTRIBUTES);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>();
        final boolean async = context.getProperty(ASYNC_INSERT).asBoolean();
        final boolean token = !TOKEN_NONE.equals(context.getProperty(DEDUPLICATION_TOKEN).getValue());
        if (async && token) {
            results.add(new ValidationResult.Builder()
                    .subject(DEDUPLICATION_TOKEN.getDisplayName())
                    .valid(false)
                    .explanation("cannot be used with Async Insert; ClickHouse ignores insert_deduplication_token on async inserts")
                    .build());
        }
        return results;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ClickHouseConnectionService service = context.getProperty(CONNECTION_SERVICE).asControllerService(ClickHouseConnectionService.class);
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final String target = targetTable(context, flowFile);
        final int maxRows = context.getProperty(MAX_ROWS_PER_INSERT).asInteger();
        final Map<String, String> settings = insertSettings(context, flowFile);

        final String token;
        try {
            token = deduplicationToken(context, flowFile);
        } catch (final IllegalArgumentException e) {
            getLogger().error("{} for {}", e.getMessage(), flowFile);
            session.transfer(session.putAttribute(flowFile, ATTR_ERROR, e.getMessage()), REL_FAILURE);
            return;
        }

        final Client client = service.getClient();
        final InsertRun run = new InsertRun(client, target, settings, token);
        try (InputStream in = session.read(flowFile);
             RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger())) {
            Record record;
            while ((record = reader.nextRecord()) != null) {
                run.add(record);
                if (run.pendingRows() >= maxRows) {
                    run.send();
                }
            }
            run.send();
        } catch (final MalformedRecordException | org.apache.nifi.schema.access.SchemaNotFoundException | IOException e) {
            getLogger().error("Could not read records from {}", flowFile, e);
            session.transfer(session.putAttribute(flowFile, ATTR_ERROR, InsertErrors.message(e)), REL_FAILURE);
            return;
        } catch (final RuntimeException e) {
            final String message = InsertErrors.message(e);
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(ATTR_ERROR, message);
            attributes.put(ATTR_QUERY_ID, String.join(",", run.queryIds()));
            if (InsertErrors.isRetryable(e)) {
                getLogger().warn("Insert into {} failed for {}, will retry: {}", target, flowFile, message);
                session.transfer(session.putAllAttributes(flowFile, attributes), REL_RETRY);
                context.yield();
            } else {
                getLogger().error("Insert into {} failed for {}: {}", target, flowFile, message);
                session.transfer(session.putAllAttributes(flowFile, attributes), REL_FAILURE);
            }
            return;
        }

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ATTR_ROWS_WRITTEN, String.valueOf(run.rowsWritten()));
        attributes.put(ATTR_INSERTS, String.valueOf(run.inserts()));
        attributes.put(ATTR_QUERY_ID, String.join(",", run.queryIds()));
        flowFile = session.putAllAttributes(flowFile, attributes);
        session.getProvenanceReporter().send(flowFile, "clickhouse://" + target, run.inserts() + " inserts, " + run.rowsWritten() + " rows");
        session.transfer(flowFile, REL_SUCCESS);
    }

    static String targetTable(final ProcessContext context, final FlowFile flowFile) {
        final String table = context.getProperty(TABLE).evaluateAttributeExpressions(flowFile).getValue();
        final String database = context.getProperty(DATABASE).evaluateAttributeExpressions(flowFile).getValue();
        return database == null || database.isBlank() ? table : database + "." + table;
    }

    /** Server settings for one FlowFile: processor defaults first, then the dynamic properties on top. */
    static Map<String, String> insertSettings(final ProcessContext context, final FlowFile flowFile) {
        final Map<String, String> settings = new java.util.LinkedHashMap<>();
        settings.put("date_time_input_format", "best_effort");
        // ClickHouse drops unknown JSON fields by default; a record field without a column is a failure here, not silent loss.
        settings.put("input_format_skip_unknown_fields", "0");
        if (context.getProperty(ASYNC_INSERT).asBoolean()) {
            settings.put("async_insert", "1");
            settings.put("wait_for_async_insert", "1");
        } else {
            settings.put("async_insert", "0");
        }
        if (context.getProperty(WAIT_END_OF_QUERY).asBoolean()) {
            settings.put("wait_end_of_query", "1");
        }
        settings.putAll(ClickHouseSettings.read(context, ExpressionLanguageScope.FLOWFILE_ATTRIBUTES, flowFile));
        return settings;
    }

    /** @return the base token, or null when deduplication is off */
    static String deduplicationToken(final ProcessContext context, final FlowFile flowFile) {
        final String mode = context.getProperty(DEDUPLICATION_TOKEN).getValue();
        return switch (mode) {
            case TOKEN_FLOWFILE_UUID -> flowFile.getAttribute("uuid");
            case TOKEN_ATTRIBUTE -> {
                final String name = context.getProperty(DEDUPLICATION_TOKEN_ATTRIBUTE).getValue();
                final String value = flowFile.getAttribute(name);
                if (value == null || value.isBlank()) {
                    throw new IllegalArgumentException("Deduplication token attribute '" + name + "' is missing or empty");
                }
                yield value;
            }
            default -> null;
        };
    }

    /** The first insert of a FlowFile uses the token as is; the following ones get the insert index. */
    static String batchToken(final String base, final int index) {
        if (base == null) {
            return null;
        }
        return index == 0 ? base : base + "-" + index;
    }

    /** Buffers records into JSONEachRow batches and sends them one insert at a time. */
    private static final class InsertRun {
        private final Client client;
        private final String target;
        private final Map<String, String> settings;
        private final String token;
        private final List<String> queryIds = new ArrayList<>();
        private ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        private JsonEachRowEncoder encoder;
        private long rowsWritten;

        InsertRun(final Client client, final String target, final Map<String, String> settings, final String token) {
            this.client = client;
            this.target = target;
            this.settings = settings;
            this.token = token;
        }

        void add(final Record record) throws IOException {
            if (encoder == null) {
                encoder = new JsonEachRowEncoder(buffer);
            }
            encoder.write(record);
        }

        long pendingRows() {
            return encoder == null ? 0 : encoder.getRowCount();
        }

        void send() throws IOException {
            if (encoder == null) {
                return;
            }
            encoder.close();
            final byte[] body = buffer.toByteArray();
            encoder = null;
            buffer = new ByteArrayOutputStream();

            final String queryId = UUID.randomUUID().toString();
            final InsertSettings insertSettings = new InsertSettings().setQueryId(queryId);
            settings.forEach(insertSettings::serverSetting);
            final String batchToken = batchToken(token, queryIds.size());
            if (batchToken != null) {
                insertSettings.setDeduplicationToken(batchToken);
            }
            queryIds.add(queryId);

            try (InsertResponse response = client.insert(target, new ByteArrayInputStream(body), ClickHouseFormat.JSONEachRow, insertSettings).join()) {
                rowsWritten += response.getWrittenRows();
            }
        }

        int inserts() {
            return queryIds.size();
        }

        long rowsWritten() {
            return rowsWritten;
        }

        List<String> queryIds() {
            return queryIds;
        }
    }
}
