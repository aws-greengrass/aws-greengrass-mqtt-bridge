/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge;

import com.aws.greengrass.componentmanager.KernelConfigResolver;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.util.Coerce;
import com.aws.greengrass.util.Utils;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Utilities for accessing MQTT Bridge configuration.
 */
public final class BridgeConfig {

    @Deprecated
    static final String KEY_DEPRECATED_BROKER_URI = "brokerServerUri";
    static final String KEY_BROKER_URI = "brokerUri";
    public static final String KEY_CLIENT_ID = "clientId";
    static final String KEY_MQTT_TOPIC_MAPPING = "mqttTopicMapping";
    public static final String KEY_BROKER_CONNECTION_OPTIONS = "brokerConnectionOptions";
    public static final String KEY_USERNAME = "username";
    public static final String KEY_PASSWORD = "password";

    static final String[] PATH_DEPRECATED_BROKER_URI =
            {KernelConfigResolver.CONFIGURATION_CONFIG_KEY, KEY_DEPRECATED_BROKER_URI};
    static final String[] PATH_BROKER_URI =
            {KernelConfigResolver.CONFIGURATION_CONFIG_KEY, KEY_BROKER_URI};
    static final String[] PATH_CLIENT_ID =
            {KernelConfigResolver.CONFIGURATION_CONFIG_KEY, KEY_CLIENT_ID};
    static final String[] PATH_MQTT_TOPIC_MAPPING =
            {KernelConfigResolver.CONFIGURATION_CONFIG_KEY, KEY_MQTT_TOPIC_MAPPING};
    private static final String[] PATH_USERNAME =
            {KernelConfigResolver.CONFIGURATION_CONFIG_KEY, KEY_BROKER_CONNECTION_OPTIONS, KEY_USERNAME};
    private static final String[] PATH_PASSWORD =
            {KernelConfigResolver.CONFIGURATION_CONFIG_KEY, KEY_BROKER_CONNECTION_OPTIONS, KEY_PASSWORD};

    private static final String DEFAULT_BROKER_URI = "ssl://localhost:8883";
    private static final String DEFAULT_CLIENT_ID = "mqtt-bridge-" + Utils.generateRandomString(11);
    private static final String DEFAULT_USERNAME = "";
    private static final String DEFAULT_PASSWORD = "";

    private BridgeConfig(){
    }

    /**
     * Get brokerUri configuration.
     *
     * @param topics config topics
     * @return brokerUri value
     * @throws URISyntaxException for invalid brokerUri format
     */
    public static URI getBrokerUri(Topics topics) throws URISyntaxException {
        // brokerUri should take precedence since brokerServerUri is deprecated
        String deprecatedBrokerUri = Coerce.toString(
                topics.findOrDefault(DEFAULT_BROKER_URI, PATH_DEPRECATED_BROKER_URI));
        String brokerUri = Coerce.toString(
                topics.findOrDefault(deprecatedBrokerUri, PATH_BROKER_URI));
        return new URI(brokerUri);
    }

    /**
     * Get clientId configuration.
     *
     * @param topics config topics
     * @return clientId value
     */
    public static String getClientId(Topics topics) {
        return Coerce.toString(topics.findOrDefault(DEFAULT_CLIENT_ID, PATH_CLIENT_ID));
    }

    /**
     * Get mqtt username configuration.
     *
     * @param topics config topics
     * @return username value
     */
    public static String getUsername(Topics topics) {
        return Coerce.toString(topics.findOrDefault(DEFAULT_USERNAME, PATH_USERNAME));
    }

    /**
     * Get mqtt password configuration.
     *
     * @param topics config topics
     * @return password value
     */
    public static String getPassword(Topics topics) {
        return Coerce.toString(topics.findOrDefault(DEFAULT_PASSWORD, PATH_PASSWORD));
    }
}
