package io.github.danmorcov88.stavilar.processors;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseColumn;
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
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Tags({"clickhouse", "query", "select", "record", "database", "sql"})
@CapabilityDescription("Runs a SQL query on ClickHouse and writes the result as records with the configured Record Writer. "
        + "The result is streamed: rows are written as they arrive, so memory does not grow with the result size. "
        + "Large results can be split into several FlowFiles.")
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@DynamicProperty(name = "ch.setting.<name>", value = "setting value",
        description = "A ClickHouse server setting sent with the query, for example ch.setting.max_execution_time = 60. "
                + "Overrides the same setting on the connection service.",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
@WritesAttributes({
        @WritesAttribute(attribute = "record.count", description = "Records in the FlowFile"),
        @WritesAttribute(attribute = "mime.type", description = "MIME type of the Record Writer"),
        @WritesAttribute(attribute = QueryClickHouseRecord.ATTR_ROWS_READ, description = "Rows read into the FlowFile"),
        @WritesAttribute(attribute = QueryClickHouseRecord.ATTR_QUERY_ID, description = "ClickHouse query id"),
        @WritesAttribute(attribute = "fragment.identifier", description = "Shared id of all FlowFiles of one query result, when split"),
        @WritesAttribute(attribute = "fragment.index", description = "Position of the FlowFile in the result, from 0, when split"),
        @WritesAttribute(attribute = "fragment.count", description = "Number of FlowFiles of the result, when split"),
        @WritesAttribute(attribute = QueryClickHouseRecord.ATTR_ERROR, description = "Error message when routed to failure")})
public class QueryClickHouseRecord extends AbstractProcessor {

    static final String ATTR_ROWS_READ = "clickhouse.rows.read";
    static final String ATTR_QUERY_ID = "clickhouse.query.id";
    static final String ATTR_ERROR = "clickhouse.error";

    public static final PropertyDescriptor CONNECTION_SERVICE = new PropertyDescriptor.Builder()
            .name("ClickHouse Connection Service")
            .required(true)
            .identifiesControllerService(ClickHouseConnectionService.class)
            .build();

    public static final PropertyDescriptor SQL_QUERY = new PropertyDescriptor.Builder()
            .name("SQL Query")
            .description("The SELECT to run. Leave empty to take the query from the content of the incoming FlowFile.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .description("Writes the result rows.")
            .required(true)
            .identifiesControllerService(RecordSetWriterFactory.class)
            .build();

    public static final PropertyDescriptor MAX_ROWS_PER_FLOWFILE = new PropertyDescriptor.Builder()
            .name("Max Rows Per FlowFile")
            .description("Split the result into FlowFiles of at most this many rows. 0 puts the whole result in one FlowFile.")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles with the query result.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("The query failed. The incoming FlowFile, or an empty FlowFile when there was none, carries the error.")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The incoming FlowFile after a successful query.")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(CONNECTION_SERVICE, SQL_QUERY, RECORD_WRITER, MAX_ROWS_PER_FLOWFILE);
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE, REL_ORIGINAL);

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
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile incoming = session.get();
        if (incoming == null && context.hasIncomingConnection()) {
            return;
        }

        final String sql = sql(context, session, incoming);
        if (sql == null || sql.isBlank()) {
            getLogger().error("No SQL Query is configured and no incoming FlowFile provides one");
            if (incoming != null) {
                session.transfer(session.putAttribute(incoming, ATTR_ERROR, "no SQL query"), REL_FAILURE);
            }
            context.yield();
            return;
        }

        final ClickHouseConnectionService service = context.getProperty(CONNECTION_SERVICE).asControllerService(ClickHouseConnectionService.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final int maxRows = context.getProperty(MAX_ROWS_PER_FLOWFILE).asInteger();
        final String queryId = UUID.randomUUID().toString();

        final QuerySettings settings = new QuerySettings()
                .setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                .setQueryId(queryId);
        ClickHouseSettings.read(context, ExpressionLanguageScope.FLOWFILE_ATTRIBUTES, incoming).forEach(settings::serverSetting);

        final Client client = service.getClient();
        final String uri = "clickhouse://" + client.getEndpoints().stream().findFirst().orElse("") + "/" + queryId;
        final List<FlowFile> results = new ArrayList<>();
        try (QueryResponse response = client.query(sql, settings).get()) {
            final ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
            final List<ClickHouseColumn> columns = reader.getSchema().getColumns();
            final RecordSchema schema = ClickHouseRecordSchema.of(reader.getSchema());
            final Map<String, String> parentAttributes = incoming == null ? Map.of() : incoming.getAttributes();
            final RecordSchema writeSchema = writerFactory.getSchema(parentAttributes, schema);
            final String fragmentId = UUID.randomUUID().toString();

            do {
                FlowFile flowFile = incoming == null ? session.create() : session.create(incoming);
                long rows = 0;
                final WriteResult result;
                final String mimeType;
                try (OutputStream out = session.write(flowFile);
                     RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out, flowFile)) {
                    writer.beginRecordSet();
                    while (reader.hasNext() && (maxRows == 0 || rows < maxRows)) {
                        writer.write(toRecord(schema, columns, reader.next()));
                        rows++;
                    }
                    result = writer.finishRecordSet();
                    mimeType = writer.getMimeType();
                }
                final Map<String, String> attributes = new HashMap<>(result.getAttributes());
                attributes.put("record.count", String.valueOf(result.getRecordCount()));
                attributes.put(CoreAttributes.MIME_TYPE.key(), mimeType);
                attributes.put(ATTR_ROWS_READ, String.valueOf(rows));
                attributes.put(ATTR_QUERY_ID, queryId);
                if (maxRows > 0) {
                    attributes.put(FragmentAttributes.FRAGMENT_ID.key(), fragmentId);
                    attributes.put(FragmentAttributes.FRAGMENT_INDEX.key(), String.valueOf(results.size()));
                }
                results.add(session.putAllAttributes(flowFile, attributes));
            } while (reader.hasNext());
        } catch (final Exception e) {
            for (final FlowFile partial : results) {
                session.remove(partial);
            }
            final String message = ClickHouseErrors.message(e);
            getLogger().error("Query {} failed: {}", queryId, message, e);
            final FlowFile failed = incoming == null ? session.create() : incoming;
            session.transfer(session.putAllAttributes(failed, Map.of(ATTR_ERROR, message, ATTR_QUERY_ID, queryId)), REL_FAILURE);
            if (ClickHouseErrors.isRetryable(e)) {
                context.yield();
            }
            return;
        }

        final long total = results.stream().mapToLong(f -> Long.parseLong(f.getAttribute(ATTR_ROWS_READ))).sum();
        for (int i = 0; i < results.size(); i++) {
            FlowFile flowFile = results.get(i);
            if (maxRows > 0) {
                flowFile = session.putAttribute(flowFile, FragmentAttributes.FRAGMENT_COUNT.key(), String.valueOf(results.size()));
            }
            if (incoming == null) {
                session.getProvenanceReporter().receive(flowFile, uri);
            } else {
                session.getProvenanceReporter().fetch(flowFile, uri);
            }
            session.transfer(flowFile, REL_SUCCESS);
        }
        getLogger().info("Query {} returned {} rows in {} FlowFiles", queryId, total, results.size());
        if (incoming != null) {
            session.transfer(session.putAttribute(incoming, ATTR_QUERY_ID, queryId), REL_ORIGINAL);
        }
    }

    private String sql(final ProcessContext context, final ProcessSession session, final FlowFile incoming) {
        if (context.getProperty(SQL_QUERY).isSet()) {
            return context.getProperty(SQL_QUERY).evaluateAttributeExpressions(incoming).getValue();
        }
        if (incoming == null) {
            return null;
        }
        try (InputStream in = session.read(incoming)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (final IOException e) {
            throw new ProcessException("Could not read the query from " + incoming, e);
        }
    }

    private static MapRecord toRecord(final RecordSchema schema, final List<ClickHouseColumn> columns, final Map<String, Object> row) {
        final Map<String, Object> values = new LinkedHashMap<>();
        for (final ClickHouseColumn column : columns) {
            values.put(column.getColumnName(), ClickHouseValues.toRecordValue(column, row.get(column.getColumnName())));
        }
        return new MapRecord(schema, values);
    }
}
