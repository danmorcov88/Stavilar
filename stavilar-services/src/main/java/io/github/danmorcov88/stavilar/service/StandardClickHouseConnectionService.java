package io.github.danmorcov88.stavilar.service;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.Protocol;
import io.github.danmorcov88.stavilar.api.ClickHouseConnectionService;
import io.github.danmorcov88.stavilar.service.EndpointsValidator.HostPort;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Tags({"clickhouse", "database", "connection", "client"})
@CapabilityDescription("Holds a ClickHouse client-v2 client over HTTP(S) and shares it with processors. "
        + "The service pings the server when enabled and stays disabled if the server cannot be reached.")
@DynamicProperty(name = "ch.setting.<name>", value = "setting value",
        description = "A ClickHouse server setting sent with every query and insert made through this client, "
                + "for example ch.setting.max_execution_time = 60. Processors can override these per operation.",
        expressionLanguageScope = ExpressionLanguageScope.ENVIRONMENT)
public class StandardClickHouseConnectionService extends AbstractControllerService implements ClickHouseConnectionService {

    static final String SETTING_PREFIX = "ch.setting.";

    public static final PropertyDescriptor ENDPOINTS = new PropertyDescriptor.Builder()
            .name("Endpoints")
            .description("Comma-separated list of host:port entries for the ClickHouse HTTP interface, "
                    + "for example ch1:8123,ch2:8123. The client fails over between them.")
            .required(true)
            .addValidator(EndpointsValidator.INSTANCE)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor USE_TLS = new PropertyDescriptor.Builder()
            .name("Use TLS")
            .description("Connect over HTTPS. Without an SSL Context Service the JVM default trust store is used.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("Provides the trust store used to verify the server certificate. "
                    + "Only the trust store is used; client certificates are not supported.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .dependsOn(USE_TLS, "true")
            .build();

    public static final PropertyDescriptor DATABASE = new PropertyDescriptor.Builder()
            .name("Database")
            .description("Default database for queries and inserts that do not name one.")
            .required(true)
            .defaultValue("default")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .required(true)
            .defaultValue("default")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .required(false)
            .sensitive(true)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor COMPRESSION = new PropertyDescriptor.Builder()
            .name("Compression")
            .description("LZ4 compresses both request and response bodies.")
            .required(true)
            .allowableValues("LZ4", "None")
            .defaultValue("LZ4")
            .build();

    public static final PropertyDescriptor CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connect Timeout")
            .description("Time to wait for a TCP connection to an endpoint.")
            .required(true)
            .defaultValue("10 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor SOCKET_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Socket Timeout")
            .description("Time to wait for data on an open connection. Long-running inserts and queries need a higher value.")
            .required(true)
            .defaultValue("30 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            ENDPOINTS, USE_TLS, SSL_CONTEXT_SERVICE, DATABASE, USERNAME, PASSWORD, COMPRESSION, CONNECT_TIMEOUT, SOCKET_TIMEOUT);

    private volatile Client client;
    private volatile Map<String, String> defaultSettings = Map.of();
    private volatile String database;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        final PropertyDescriptor.Builder builder = new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT);
        if (propertyDescriptorName.startsWith(SETTING_PREFIX) && propertyDescriptorName.length() > SETTING_PREFIX.length()) {
            return builder
                    .description("ClickHouse server setting " + propertyDescriptorName.substring(SETTING_PREFIX.length()))
                    .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                    .build();
        }
        return builder
                .addValidator((subject, input, context) -> new ValidationResult.Builder()
                        .subject(subject).input(input).valid(false)
                        .explanation("dynamic properties must be named " + SETTING_PREFIX + "<setting name>")
                        .build())
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>();
        final SSLContextService sslService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        if (sslService != null && !sslService.isTrustStoreConfigured()) {
            results.add(new ValidationResult.Builder()
                    .subject(SSL_CONTEXT_SERVICE.getDisplayName())
                    .valid(false)
                    .explanation("the SSL Context Service has no trust store configured")
                    .build());
        }
        return results;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        final List<HostPort> endpoints = EndpointsValidator.parse(
                context.getProperty(ENDPOINTS).evaluateAttributeExpressions().getValue());
        final boolean useTls = context.getProperty(USE_TLS).asBoolean();
        final String db = context.getProperty(DATABASE).evaluateAttributeExpressions().getValue();
        final Map<String, String> settings = readSettings(context);

        final Client.Builder builder = new Client.Builder()
                .setUsername(context.getProperty(USERNAME).evaluateAttributeExpressions().getValue())
                .setPassword(context.getProperty(PASSWORD).isSet() ? context.getProperty(PASSWORD).getValue() : "")
                .setDefaultDatabase(db)
                .setConnectTimeout(context.getProperty(CONNECT_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS), ChronoUnit.MILLIS)
                .setSocketTimeout(context.getProperty(SOCKET_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS), ChronoUnit.MILLIS)
                .setClientName(clientName());
        for (final HostPort endpoint : endpoints) {
            builder.addEndpoint(Protocol.HTTP, endpoint.host(), endpoint.port(), useTls);
        }

        final boolean lz4 = "LZ4".equals(context.getProperty(COMPRESSION).getValue());
        builder.compressClientRequest(lz4).compressServerResponse(lz4);

        final SSLContextService sslService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        if (useTls && sslService != null) {
            builder.setSSLTrustStore(sslService.getTrustStoreFile())
                    .setSSLTrustStorePassword(sslService.getTrustStorePassword())
                    .setSSLTrustStoreType(sslService.getTrustStoreType());
        }

        settings.forEach(builder::serverSetting);

        final Client newClient = builder.build();
        try {
            if (!newClient.ping()) {
                throw new InitializationException("ClickHouse did not answer the ping on " + endpoints);
            }
        } catch (final InitializationException e) {
            newClient.close();
            throw e;
        } catch (final RuntimeException e) {
            newClient.close();
            throw new InitializationException("Could not connect to ClickHouse on " + endpoints + ": " + e.getMessage(), e);
        }

        client = newClient;
        defaultSettings = settings;
        database = db;
        getLogger().info("Connected to ClickHouse {} on {}", newClient.getServerVersion(), endpoints);
    }

    @OnDisabled
    public void shutdown() {
        final Client current = client;
        client = null;
        if (current != null) {
            current.close();
        }
    }

    @Override
    public Client getClient() {
        final Client current = client;
        if (current == null) {
            throw new IllegalStateException("ClickHouseConnectionService is not enabled");
        }
        return current;
    }

    @Override
    public Map<String, String> getDefaultSettings() {
        return defaultSettings;
    }

    @Override
    public String getDatabase() {
        return database;
    }

    private static Map<String, String> readSettings(final ConfigurationContext context) {
        final Map<String, String> settings = new LinkedHashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.isDynamic() && descriptor.getName().startsWith(SETTING_PREFIX)) {
                final String value = context.getProperty(descriptor).evaluateAttributeExpressions().getValue();
                settings.put(descriptor.getName().substring(SETTING_PREFIX.length()), value);
            }
        }
        return Collections.unmodifiableMap(settings);
    }

    private String clientName() {
        final String version = getClass().getPackage().getImplementationVersion();
        return version == null ? "stavilar" : "stavilar/" + version;
    }
}
