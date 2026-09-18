package io.github.danmorcov88.stavilar.processors;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.command.CommandResponse;
import com.clickhouse.client.api.command.CommandSettings;
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
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Tags({"clickhouse", "sql", "ddl", "execute", "statement", "database"})
@CapabilityDescription("Runs one SQL statement on ClickHouse that returns no rows: CREATE, ALTER, DROP, TRUNCATE, OPTIMIZE, "
        + "INSERT ... SELECT and the like. The statement comes from the SQL Statement property or from the FlowFile content. "
        + "ClickHouse's X-ClickHouse-Summary header (rows read and written, elapsed time, memory) is written as attributes.")
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@DynamicProperty(name = "ch.setting.<name>", value = "setting value",
        description = "A ClickHouse server setting sent with the statement, for example ch.setting.mutations_sync = 1 "
                + "to wait for an ALTER ... DELETE to finish. Overrides the same setting on the connection service.",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
@WritesAttributes({
        @WritesAttribute(attribute = ExecuteClickHouseStatement.ATTR_QUERY_ID, description = "ClickHouse query id of the statement"),
        @WritesAttribute(attribute = "clickhouse.summary.<key>", description = "One attribute per key of X-ClickHouse-Summary: "
                + "read_rows, read_bytes, written_rows, written_bytes, result_rows, result_bytes, elapsed_ns, memory_usage, ..."),
        @WritesAttribute(attribute = ExecuteClickHouseStatement.ATTR_ERROR, description = "Error message when routed to failure or retry")})
public class ExecuteClickHouseStatement extends AbstractProcessor {

    static final String ATTR_QUERY_ID = "clickhouse.query.id";
    static final String ATTR_ERROR = "clickhouse.error";

    public static final PropertyDescriptor CONNECTION_SERVICE = new PropertyDescriptor.Builder()
            .name("ClickHouse Connection Service")
            .required(true)
            .identifiesControllerService(ClickHouseConnectionService.class)
            .build();

    public static final PropertyDescriptor SQL_STATEMENT = new PropertyDescriptor.Builder()
            .name("SQL Statement")
            .description("One statement to run. Leave empty to take it from the content of the incoming FlowFile. "
                    + "ClickHouse runs one statement per request; do not put several statements separated by ';'.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The statement ran. The incoming FlowFile, or a new empty one, carries the query id and the summary.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("ClickHouse rejected the statement (syntax, missing table, permissions).")
            .build();

    public static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("A network problem or a temporary server condition stopped the statement.")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(CONNECTION_SERVICE, SQL_STATEMENT);
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
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile incoming = session.get();
        if (incoming == null && context.hasIncomingConnection()) {
            return;
        }

        final String sql = statement(context, session, incoming);
        if (sql == null || sql.isBlank()) {
            getLogger().error("No SQL Statement is configured and no incoming FlowFile provides one");
            if (incoming != null) {
                session.transfer(session.putAttribute(incoming, ATTR_ERROR, "no SQL statement"), REL_FAILURE);
            }
            context.yield();
            return;
        }

        final ClickHouseConnectionService service = context.getProperty(CONNECTION_SERVICE).asControllerService(ClickHouseConnectionService.class);
        final String queryId = UUID.randomUUID().toString();
        final CommandSettings settings = new CommandSettings();
        settings.setQueryId(queryId);
        ClickHouseSettings.read(context, ExpressionLanguageScope.FLOWFILE_ATTRIBUTES, incoming).forEach(settings::serverSetting);

        final Client client = service.getClient();
        final FlowFile flowFile = incoming == null ? session.create() : incoming;
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ATTR_QUERY_ID, queryId);
        try (CommandResponse response = client.execute(sql, settings).get()) {
            attributes.putAll(ClickHouseSummary.toAttributes(response.getResponseHeaders()));
        } catch (final Exception e) {
            final String message = ClickHouseErrors.message(e);
            attributes.put(ATTR_ERROR, message);
            if (ClickHouseErrors.isRetryable(e)) {
                getLogger().warn("Statement {} failed, will retry: {}", queryId, message);
                session.transfer(session.putAllAttributes(flowFile, attributes), REL_RETRY);
                context.yield();
            } else {
                getLogger().error("Statement {} failed: {}", queryId, message);
                session.transfer(session.putAllAttributes(flowFile, attributes), REL_FAILURE);
            }
            return;
        }

        final FlowFile done = session.putAllAttributes(flowFile, attributes);
        session.getProvenanceReporter().send(done, "clickhouse://" + client.getEndpoints().stream().findFirst().orElse("") + "/" + queryId);
        session.transfer(done, REL_SUCCESS);
    }

    private String statement(final ProcessContext context, final ProcessSession session, final FlowFile incoming) {
        if (context.getProperty(SQL_STATEMENT).isSet()) {
            return context.getProperty(SQL_STATEMENT).evaluateAttributeExpressions(incoming).getValue();
        }
        if (incoming == null) {
            return null;
        }
        try (InputStream in = session.read(incoming)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (final IOException e) {
            throw new ProcessException("Could not read the statement from " + incoming, e);
        }
    }
}
