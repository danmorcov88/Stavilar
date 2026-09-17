package io.github.danmorcov88.stavilar.api;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * ClickHouse server settings given as dynamic properties named {@code ch.setting.<name>}.
 * Shared by the connection service and the processors.
 */
public final class ClickHouseSettings {

    /** Dynamic property prefix: {@code ch.setting.<name>} maps to server setting {@code <name>}. */
    public static final String PREFIX = "ch.setting.";

    private ClickHouseSettings() {
    }

    /** @return true when the dynamic property name is a setting with a non-empty name */
    public static boolean isSetting(final String propertyName) {
        return propertyName.startsWith(PREFIX) && propertyName.length() > PREFIX.length();
    }

    /** @return the server setting name for a property name that passed {@link #isSetting} */
    public static String settingName(final String propertyName) {
        return propertyName.substring(PREFIX.length());
    }

    /**
     * Descriptor for a dynamic property. Names with the prefix become settings with a
     * non-blank value; any other name is rejected with a clear message.
     */
    public static PropertyDescriptor dynamicProperty(final String propertyName, final ExpressionLanguageScope scope) {
        final PropertyDescriptor.Builder builder = new PropertyDescriptor.Builder()
                .name(propertyName)
                .dynamic(true)
                .expressionLanguageSupported(scope);
        if (isSetting(propertyName)) {
            return builder
                    .description("ClickHouse server setting " + settingName(propertyName))
                    .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                    .build();
        }
        return builder
                .addValidator((subject, input, context) -> new ValidationResult.Builder()
                        .subject(subject).input(input).valid(false)
                        .explanation("dynamic properties must be named " + PREFIX + "<setting name>")
                        .build())
                .build();
    }

    /**
     * Reads all {@code ch.setting.*} properties. Expression Language is evaluated against
     * the FlowFile when one is given, otherwise against the environment only.
     *
     * @return unmodifiable map of setting name to value, in property order
     */
    public static Map<String, String> read(final PropertyContext context, final ExpressionLanguageScope scope, final FlowFile flowFile) {
        final Map<String, String> settings = new LinkedHashMap<>();
        for (final String name : context.getAllProperties().keySet()) {
            if (isSetting(name)) {
                final PropertyValue raw = context.getProperty(dynamicProperty(name, scope));
                final PropertyValue value = flowFile == null ? raw.evaluateAttributeExpressions() : raw.evaluateAttributeExpressions(flowFile);
                settings.put(settingName(name), value.getValue());
            }
        }
        return Collections.unmodifiableMap(settings);
    }
}
