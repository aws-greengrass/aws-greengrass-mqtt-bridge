/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge;

import com.amazon.aws.iot.greengrass.component.common.SerializerFactory;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqtt.bridge.model.InvalidConfigurationException;
import com.aws.greengrass.mqtt.bridge.model.Mqtt5RouteOptions;
import com.aws.greengrass.mqtt.bridge.model.MqttVersion;
import com.aws.greengrass.util.Coerce;
import com.aws.greengrass.util.Utils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Utilities for accessing MQTT Bridge configuration.
 */
@Getter
@ToString
@EqualsAndHashCode
@Builder(toBuilder = true)
@RequiredArgsConstructor
public final class BridgeConfig {
    private static final Logger LOGGER = LogManager.getLogger(BridgeConfig.class);
    private static final ObjectMapper OBJECT_MAPPER = SerializerFactory.getConfigurationSerializerJson();
    private static final String INVALID_CONFIG_LOG_FORMAT_STRING = "Provided {} out of range. Defaulting to {}";

    static final String KEY_BROKER_SERVER_URI = "brokerServerUri"; // for backwards compatibility only
    public static final String KEY_BROKER_URI = "brokerUri";
    public static final String KEY_CLIENT_ID = "clientId";
    static final String KEY_ACK_TIMEOUT_SECONDS = "ackTimeoutSeconds";
    static final String KEY_CONNACK_TIMEOUT_MS = "connAckTimeoutMs";
    static final String KEY_PING_TIMEOUT_MS = "pingTimeoutMs";
    static final String KEY_MAX_RECONNECT_DELAY_MS = "maxReconnectDelayMs";
    static final String KEY_MIN_RECONNECT_DELAY_MS = "minReconnectDelayMs";
    public static final String KEY_MQTT_TOPIC_MAPPING = "mqttTopicMapping";
    public static final String KEY_RECEIVE_MAXIMUM = "receiveMaximum";
    public static final String KEY_MAXIMUM_PACKET_SIZE = "maximumPacketSize";
    public static final String KEY_SESSION_EXPIRY_INTERVAL = "sessionExpiryInterval";
    public static final String KEY_MQTT_5_ROUTE_OPTIONS = "mqtt5RouteOptions";
    static final String KEY_BROKER_CLIENT = "brokerClient";
    static final String KEY_VERSION = "version";


    private static final long MIN_TIMEOUT = 0L;
    private static final int MIN_RECEIVE_MAXIMUM = 1;
    private static final int MAX_RECEIVE_MAXIMUM = 65_535;
    private static final long MIN_MAXIMUM_PACKET_SIZE = 1L;
    private static final long MAX_MAXIMUM_PACKET_SIZE = 4_294_967_295L;
    private static final long MIN_SESSION_EXPIRY_INTERVAL = 0L;
    private static final long MAX_SESSION_EXPIRY_INTERVAL = 4_294_967_295L;

    private static final String DEFAULT_BROKER_URI = "ssl://localhost:8883";
    private static final String DEFAULT_CLIENT_ID = "mqtt-bridge-" + Utils.generateRandomString(11);
    private static final MqttVersion DEFAULT_MQTT_VERSION = MqttVersion.MQTT3;
    public static final int DEFAULT_RECEIVE_MAXIMUM = MAX_RECEIVE_MAXIMUM;
    public static final Long DEFAULT_MAXIMUM_PACKET_SIZE = null;
    public static final long DEFAULT_SESSION_EXPIRY_INTERVAL = MAX_SESSION_EXPIRY_INTERVAL;
    private static final long DEFAULT_ACK_TIMEOUT_SECONDS = 60L;
    private static final long DEFAULT_CONNACK_TIMEOUT_MS = Duration.ofSeconds(20).toMillis();
    private static final long DEFAULT_PING_TIMEOUT_MS = Duration.ofSeconds(30).toMillis();
    private static final long DEFAULT_MAX_RECONNECT_DELAY_MS = Duration.ofSeconds(30).toMillis();
    private static final long DEFAULT_MIN_RECONNECT_DELAY_MS = Duration.ofSeconds(1).toMillis();

    private final URI brokerUri;
    private final String clientId;
    private final long ackTimeoutSeconds;
    private final long connAckTimeoutMs;
    private final long pingTimeoutMs;
    private final long maxReconnectDelayMs;
    private final long minReconnectDelayMs;
    private final Map<String, TopicMapping.MappingEntry> topicMapping;
    private final Map<String, Mqtt5RouteOptions> mqtt5RouteOptions;
    private final MqttVersion mqttVersion;
    private final int receiveMaximum;
    private final Long maximumPacketSize;
    private final long sessionExpiryInterval;


    /**
     * Create a BridgeConfig from configuration topics.
     *
     * @param configurationTopics configuration topics
     * @return bridge config
     * @throws InvalidConfigurationException if config from topics is invalid
     */
    @SuppressWarnings("PMD.PrematureDeclaration")
    public static BridgeConfig fromTopics(Topics configurationTopics) throws InvalidConfigurationException {
        long maxReconnectDelayMs = getMaxReconnectDelayMs(configurationTopics);
        long minReconnectDelayMs = getMinReconnectDelayMs(configurationTopics);
        if (maxReconnectDelayMs < minReconnectDelayMs) {
            throw new InvalidConfigurationException(
                    "maxReconnectDelayMs must be greater than or equal to minReconnectDelayMs");
        }

        return BridgeConfig.builder()
                .brokerUri(getBrokerUri(configurationTopics))
                .clientId(getClientId(configurationTopics))
                .ackTimeoutSeconds(getAckTimeoutSeconds(configurationTopics))
                .connAckTimeoutMs(getConnAckTimeoutMs(configurationTopics))
                .pingTimeoutMs(getPingTimeoutMs(configurationTopics))
                .maxReconnectDelayMs(maxReconnectDelayMs)
                .minReconnectDelayMs(minReconnectDelayMs)
                .topicMapping(getTopicMapping(configurationTopics))
                .mqtt5RouteOptions(getMqtt5RouteOptions(configurationTopics))
                .mqttVersion(getMqttVersion(configurationTopics))
                .receiveMaximum(getReceiveMaximum(configurationTopics))
                .maximumPacketSize(getMaximumPacketSize(configurationTopics))
                .sessionExpiryInterval(getSessionExpiryInterval(configurationTopics))
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

    private static long getAckTimeoutSeconds(Topics configurationTopics) {
        long ackTimeoutSeconds = Coerce.toLong(configurationTopics.findOrDefault(DEFAULT_ACK_TIMEOUT_SECONDS,
                KEY_BROKER_CLIENT, KEY_ACK_TIMEOUT_SECONDS));
        if (ackTimeoutSeconds < MIN_TIMEOUT) {
            LOGGER.atWarn().kv(KEY_ACK_TIMEOUT_SECONDS, ackTimeoutSeconds)
                    .log(INVALID_CONFIG_LOG_FORMAT_STRING, KEY_ACK_TIMEOUT_SECONDS, DEFAULT_ACK_TIMEOUT_SECONDS);
            return DEFAULT_ACK_TIMEOUT_SECONDS;
        }
        return ackTimeoutSeconds;
    }

    private static long getConnAckTimeoutMs(Topics configurationTopics) {
        long connackTimeoutSeconds = Coerce.toLong(configurationTopics.findOrDefault(DEFAULT_CONNACK_TIMEOUT_MS,
                KEY_BROKER_CLIENT, KEY_CONNACK_TIMEOUT_MS));
        if (connackTimeoutSeconds < MIN_TIMEOUT) {
            LOGGER.atWarn().kv(KEY_CONNACK_TIMEOUT_MS, connackTimeoutSeconds)
                    .log(INVALID_CONFIG_LOG_FORMAT_STRING, KEY_CONNACK_TIMEOUT_MS, DEFAULT_CONNACK_TIMEOUT_MS);
            return DEFAULT_CONNACK_TIMEOUT_MS;
        }
        return connackTimeoutSeconds;
    }

    private static long getPingTimeoutMs(Topics configurationTopics) {
        long pingTimeoutMs = Coerce.toLong(configurationTopics.findOrDefault(DEFAULT_PING_TIMEOUT_MS,
                KEY_BROKER_CLIENT, KEY_PING_TIMEOUT_MS));
        if (pingTimeoutMs < MIN_TIMEOUT) {
            LOGGER.atWarn().kv(KEY_PING_TIMEOUT_MS, pingTimeoutMs)
                    .log(INVALID_CONFIG_LOG_FORMAT_STRING, KEY_PING_TIMEOUT_MS, DEFAULT_PING_TIMEOUT_MS);
            return DEFAULT_PING_TIMEOUT_MS;
        }
        return pingTimeoutMs;
    }

    private static long getMaxReconnectDelayMs(Topics configurationTopics) {
        long maxReconnectDelayMs = Coerce.toLong(configurationTopics.findOrDefault(DEFAULT_MAX_RECONNECT_DELAY_MS,
                KEY_BROKER_CLIENT, KEY_MAX_RECONNECT_DELAY_MS));
        if (maxReconnectDelayMs < MIN_TIMEOUT) {
            LOGGER.atWarn().kv(KEY_MAX_RECONNECT_DELAY_MS, maxReconnectDelayMs)
                    .log(INVALID_CONFIG_LOG_FORMAT_STRING, KEY_MAX_RECONNECT_DELAY_MS, DEFAULT_MAX_RECONNECT_DELAY_MS);
            return DEFAULT_MAX_RECONNECT_DELAY_MS;
        }
        return maxReconnectDelayMs;
    }

    private static long getMinReconnectDelayMs(Topics configurationTopics) {
        long minReconnectDelayMs = Coerce.toLong(configurationTopics.findOrDefault(DEFAULT_MIN_RECONNECT_DELAY_MS,
                KEY_BROKER_CLIENT, KEY_MIN_RECONNECT_DELAY_MS));
        if (minReconnectDelayMs < MIN_TIMEOUT) {
            LOGGER.atWarn().kv(KEY_MIN_RECONNECT_DELAY_MS, minReconnectDelayMs)
                    .log(INVALID_CONFIG_LOG_FORMAT_STRING, KEY_MIN_RECONNECT_DELAY_MS, DEFAULT_MIN_RECONNECT_DELAY_MS);
            return DEFAULT_MIN_RECONNECT_DELAY_MS;
        }
        return minReconnectDelayMs;
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

    private static Map<String, Mqtt5RouteOptions> getMqtt5RouteOptions(Topics configurationTopics)
            throws InvalidConfigurationException {
        try {
            return OBJECT_MAPPER.convertValue(
                    configurationTopics.lookupTopics(KEY_MQTT_5_ROUTE_OPTIONS).toPOJO(),
                    new TypeReference<Map<String, Mqtt5RouteOptions>>() {
                    });
        } catch (IllegalArgumentException e) {
            throw new InvalidConfigurationException("Malformed " + KEY_MQTT_5_ROUTE_OPTIONS, e);
        }
    }

    private static MqttVersion getMqttVersion(Topics configurationTopics) {
        return Coerce.toEnum(MqttVersion.class,
                Coerce.toString(configurationTopics.findOrDefault(DEFAULT_MQTT_VERSION.name(),
                        KEY_BROKER_CLIENT, KEY_VERSION)).toUpperCase(),
                DEFAULT_MQTT_VERSION);
    }

    private static int getReceiveMaximum(Topics configurationTopics) {
        int receiveMaximum = Coerce.toInt(configurationTopics.findOrDefault(DEFAULT_RECEIVE_MAXIMUM,
                KEY_BROKER_CLIENT, KEY_RECEIVE_MAXIMUM));
        if (receiveMaximum < MIN_RECEIVE_MAXIMUM) {
            LOGGER.atWarn().kv(KEY_RECEIVE_MAXIMUM, receiveMaximum)
                    .log(INVALID_CONFIG_LOG_FORMAT_STRING, KEY_RECEIVE_MAXIMUM, MIN_RECEIVE_MAXIMUM);
            return MIN_RECEIVE_MAXIMUM;
        }
        if (receiveMaximum > MAX_RECEIVE_MAXIMUM) {
            LOGGER.atWarn().kv(KEY_RECEIVE_MAXIMUM, receiveMaximum)
                    .log(INVALID_CONFIG_LOG_FORMAT_STRING, KEY_RECEIVE_MAXIMUM, MAX_RECEIVE_MAXIMUM);
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
                    .log(INVALID_CONFIG_LOG_FORMAT_STRING, KEY_MAXIMUM_PACKET_SIZE, MIN_MAXIMUM_PACKET_SIZE);
            return MIN_MAXIMUM_PACKET_SIZE;
        }
        if (maximumPacketSize > MAX_MAXIMUM_PACKET_SIZE) {
            LOGGER.atWarn().kv(KEY_MAXIMUM_PACKET_SIZE, maximumPacketSize)
                    .log(INVALID_CONFIG_LOG_FORMAT_STRING, KEY_MAXIMUM_PACKET_SIZE, MAX_MAXIMUM_PACKET_SIZE);
            return MAX_MAXIMUM_PACKET_SIZE;
        }
        return maximumPacketSize;
    }

    private static long getSessionExpiryInterval(Topics configurationTopics) {
        long sessionExpiryInterval = Coerce.toLong(configurationTopics.findOrDefault(DEFAULT_SESSION_EXPIRY_INTERVAL,
                KEY_BROKER_CLIENT, KEY_SESSION_EXPIRY_INTERVAL));
        if (sessionExpiryInterval < MIN_SESSION_EXPIRY_INTERVAL) {
            LOGGER.atWarn().kv(KEY_SESSION_EXPIRY_INTERVAL, sessionExpiryInterval)
                    .log(INVALID_CONFIG_LOG_FORMAT_STRING, KEY_SESSION_EXPIRY_INTERVAL, MIN_SESSION_EXPIRY_INTERVAL);
            return MIN_SESSION_EXPIRY_INTERVAL;
        }
        if (sessionExpiryInterval > MAX_SESSION_EXPIRY_INTERVAL) {
            LOGGER.atWarn().kv(KEY_SESSION_EXPIRY_INTERVAL, sessionExpiryInterval)
                    .log(INVALID_CONFIG_LOG_FORMAT_STRING, KEY_SESSION_EXPIRY_INTERVAL, MAX_SESSION_EXPIRY_INTERVAL);
            return MAX_SESSION_EXPIRY_INTERVAL;
        }
        return sessionExpiryInterval;
    }

    /**
     * Get mqtt5 route options for a {@link com.aws.greengrass.mqtt.bridge.TopicMapping.TopicType},
     * grouped by topic names.
     *
     * <p>If bridge is not configured for mqtt5, no options will be returned.
     *
     * @param source source type
     * @return route options by topic name
     */
    public Map<String, Mqtt5RouteOptions> getMqtt5RouteOptionsForSource(TopicMapping.TopicType source) {
        if (mqttVersion != MqttVersion.MQTT5) {
            return Collections.emptyMap();
        }
        Map<String, Mqtt5RouteOptions> opts = new HashMap<>();
        for (Map.Entry<String, TopicMapping.MappingEntry> entry : topicMapping.entrySet()) {
            String route = entry.getKey();
            TopicMapping.MappingEntry mapping = entry.getValue();
            if (mapping.getSource() != source) {
                continue;
            }
            if (!mqtt5RouteOptions.containsKey(entry.getKey())) {
                continue;
            }
            opts.put(mapping.getTopic(), mqtt5RouteOptions.get(route));
        }
        return opts;
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
                || !Objects.equals(getMqttVersion(), newConfig.getMqttVersion())
                || !Objects.equals(getSessionExpiryInterval(), newConfig.getSessionExpiryInterval())
                || !Objects.equals(getMaximumPacketSize(), newConfig.getMaximumPacketSize())
                || !Objects.equals(getReceiveMaximum(), newConfig.getReceiveMaximum());
    }
}
