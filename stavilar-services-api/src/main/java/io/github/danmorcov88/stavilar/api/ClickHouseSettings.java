package io.github.danmorcov88.stavilar.api;

/**
 * Naming shared by the service and the processors for ClickHouse server settings
 * given as dynamic properties.
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
}
