/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge;

import com.aws.greengrass.config.Topics;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqtt.bridge.model.InvalidConfigurationException;
import com.aws.greengrass.mqtt.bridge.model.MqttVersion;
import com.aws.greengrass.util.Coerce;
import com.aws.greengrass.util.Utils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;

/**
 * Utilities for accessing MQTT Bridge configuration.
 */
@Getter
@ToString
@EqualsAndHashCode
@Builder
@RequiredArgsConstructor
public final class BridgeConfig {
    private static final Logger LOGGER = LogManager.getLogger(BridgeConfig.class);

    private static final JsonMapper OBJECT_MAPPER =
            JsonMapper.builder().enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES).build();

    static final String KEY_BROKER_SERVER_URI = "brokerServerUri"; // for backwards compatibility only
    public static final String KEY_BROKER_URI = "brokerUri";
    public static final String KEY_CLIENT_ID = "clientId";
    public static final String KEY_MQTT_TOPIC_MAPPING = "mqttTopicMapping";
    static final String KEY_BROKER_CLIENT = "brokerClient";
    static final String KEY_VERSION = "version";
    static final String KEY_NO_LOCAL = "noLocal";
    static final String KEY_RECEIVE_MAXIMUM = "receiveMaximum";
    static final String KEY_MAXIMUM_PACKET_SIZE = "maximumPacketSize";

    private static final String DEFAULT_BROKER_URI = "ssl://localhost:8883";
    private static final String DEFAULT_CLIENT_ID = "mqtt-bridge-" + Utils.generateRandomString(11);
    private static final MqttVersion DEFAULT_MQTT_VERSION = MqttVersion.MQTT3;
    private static final boolean DEFAULT_NO_LOCAL = false;
    private static final int DEFAULT_RECEIVE_MAXIMUM = 65_535;
    private static final Long DEFAULT_MAXIMUM_PACKET_SIZE = null;

    private static final int MIN_RECEIVE_MAXIMUM = 1;
    private static final int MAX_RECEIVE_MAXIMUM = 65535;
    private static final long MIN_MAXIMUM_PACKET_SIZE = 1;
    private static final long MAX_MAXIMUM_PACKET_SIZE = 4294967295L;

    private final URI brokerUri;
    private final String clientId;
    private final Map<String, TopicMapping.MappingEntry> topicMapping;
    private final MqttVersion mqttVersion;
    private final boolean noLocal;
    private final int receiveMaximum;
    private final Long maximumPacketSize;


    /**
     * Create a BridgeConfig from configuration topics.
     *
     * @param configurationTopics configuration topics
     * @return bridge config
     * @throws InvalidConfigurationException if config from topics is invalid
     */
    @SuppressWarnings("PMD.PrematureDeclaration")
    public static BridgeConfig fromTopics(Topics configurationTopics) throws InvalidConfigurationException {
        return BridgeConfig.builder()
                .brokerUri(getBrokerUri(configurationTopics))
                .clientId(getClientId(configurationTopics))
                .topicMapping(getTopicMapping(configurationTopics))
                .mqttVersion(getMqttVersion(configurationTopics))
                .noLocal(getNoLocal(configurationTopics))
                .receiveMaximum(getReceiveMaximum(configurationTopics))
                .maximumPacketSize(getMaximumPacketSize(configurationTopics))
                .build();
    }

    private static URI getBrokerUri(Topics configurationTopics) throws InvalidConfigurationException {
        // brokerUri takes precedence since brokerServerUri is deprecated
        String brokerServerUri = Coerce.toString(
                configurationTopics.findOrDefault(DEFAULT_BROKER_URI, KEY_BROKER_SERVER_URI));
        String brokerUri = Coerce.toString(
                configurationTopics.findOrDefault(brokerServerUri, KEY_BROKER_URI));
        try {
            return new URI(brokerUri);
        } catch (URISyntaxException e) {
            throw new InvalidConfigurationException("Malformed " + KEY_BROKER_URI + ": " + brokerUri, e);
        }
    }

    private static String getClientId(Topics configurationTopics) {
        return Coerce.toString(configurationTopics.findOrDefault(DEFAULT_CLIENT_ID, KEY_CLIENT_ID));
    }

    private static Map<String, TopicMapping.MappingEntry> getTopicMapping(Topics configurationTopics)
            throws InvalidConfigurationException {
        try {
            return OBJECT_MAPPER.convertValue(
                    configurationTopics.lookupTopics(KEY_MQTT_TOPIC_MAPPING).toPOJO(),
                    new TypeReference<Map<String, TopicMapping.MappingEntry>>() {
                    });
        } catch (IllegalArgumentException e) {
            throw new InvalidConfigurationException("Malformed " + KEY_MQTT_TOPIC_MAPPING, e);
        }
    }

    private static MqttVersion getMqttVersion(Topics configurationTopics) {
        return Coerce.toEnum(MqttVersion.class,
                Coerce.toString(configurationTopics.findOrDefault(DEFAULT_MQTT_VERSION.name(),
                        KEY_BROKER_CLIENT, KEY_VERSION)).toUpperCase(),
                DEFAULT_MQTT_VERSION);
    }

    private static boolean getNoLocal(Topics configurationTopics) {
        return Coerce.toBoolean(configurationTopics.findOrDefault(DEFAULT_NO_LOCAL,
                KEY_BROKER_CLIENT, KEY_NO_LOCAL));
    }

    private static int getReceiveMaximum(Topics configurationTopics) {
        int receiveMaximum = Coerce.toInt(configurationTopics.findOrDefault(DEFAULT_RECEIVE_MAXIMUM,
                KEY_BROKER_CLIENT, KEY_RECEIVE_MAXIMUM));
        if (receiveMaximum < MIN_RECEIVE_MAXIMUM) {
            LOGGER.atWarn().kv(KEY_RECEIVE_MAXIMUM, receiveMaximum)
                    .log("Provided " + KEY_RECEIVE_MAXIMUM + " out of range. "
                            + "Defaulting to " + MIN_RECEIVE_MAXIMUM);
            return MIN_RECEIVE_MAXIMUM;
        }
        if (receiveMaximum > MAX_RECEIVE_MAXIMUM) {
            LOGGER.atWarn().kv(KEY_RECEIVE_MAXIMUM, receiveMaximum)
                    .log("Provided " + KEY_RECEIVE_MAXIMUM + " out of range. "
                            + "Defaulting to " + MAX_RECEIVE_MAXIMUM);
            return MAX_RECEIVE_MAXIMUM;
        }
        return receiveMaximum;
    }

    private static Long getMaximumPacketSize(Topics configurationTopics) {
        String maximumPacketSizeConf = Coerce.toString(configurationTopics.findOrDefault(DEFAULT_MAXIMUM_PACKET_SIZE,
                KEY_BROKER_CLIENT, KEY_MAXIMUM_PACKET_SIZE));
        if (maximumPacketSizeConf == null) {
            return null;
        }
        long maximumPacketSize = Coerce.toLong(maximumPacketSizeConf);
        if (maximumPacketSize < MIN_MAXIMUM_PACKET_SIZE) {
            LOGGER.atWarn().kv(KEY_MAXIMUM_PACKET_SIZE, maximumPacketSize)
                    .log("Provided " + KEY_MAXIMUM_PACKET_SIZE + " out of range. "
                            + "Defaulting to " + MIN_MAXIMUM_PACKET_SIZE);
            return MIN_MAXIMUM_PACKET_SIZE;
        }
        if (maximumPacketSize > MAX_MAXIMUM_PACKET_SIZE) {
            LOGGER.atWarn().kv(KEY_MAXIMUM_PACKET_SIZE, maximumPacketSize)
                    .log("Provided " + KEY_MAXIMUM_PACKET_SIZE + " out of range. "
                            + "Defaulting to " + MAX_MAXIMUM_PACKET_SIZE);
            return MAX_MAXIMUM_PACKET_SIZE;
        }
        return maximumPacketSize;
    }

    /**
     * Determine if the component needs to be reinstalled, based on configuration change.
     *
     * @param newConfig new configuration
     * @return true if the component should be reinstalled
     */
    public boolean reinstallRequired(BridgeConfig newConfig) {
        return !Objects.equals(getBrokerUri(), newConfig.getBrokerUri())
                || !Objects.equals(getClientId(), newConfig.getClientId())
                || !Objects.equals(getMqttVersion(), newConfig.getMqttVersion());
    }
}
