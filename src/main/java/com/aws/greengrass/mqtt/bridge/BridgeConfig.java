/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge;

import com.aws.greengrass.config.Topics;
import com.aws.greengrass.mqtt.bridge.model.InvalidConfigurationException;
import com.aws.greengrass.util.Coerce;
import com.aws.greengrass.util.Utils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
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
@RequiredArgsConstructor
public final class BridgeConfig {
    private static final JsonMapper OBJECT_MAPPER =
            JsonMapper.builder().enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES).build();

    static final String KEY_BROKER_SERVER_URI = "brokerServerUri"; // for backwards compatibility only
    public static final String KEY_BROKER_URI = "brokerUri";
    public static final String KEY_CLIENT_ID = "clientId";
    static final String KEY_MQTT_TOPIC_MAPPING = "mqttTopicMapping";

    private static final String DEFAULT_BROKER_URI = "ssl://localhost:8883";
    private static final String DEFAULT_CLIENT_ID = "mqtt-bridge-" + Utils.generateRandomString(11);

    private final URI brokerUri;
    private final String clientId;
    private final Map<String, TopicMapping.MappingEntry> topicMapping;

    public static BridgeConfig fromTopics(Topics configurationTopics) throws InvalidConfigurationException {
        URI brokerUri;
        try {
            brokerUri = getBrokerUri(configurationTopics);
        } catch (URISyntaxException e) {
            throw new InvalidConfigurationException("Malformed brokerUri", e);
        }
        String clientId = getClientId(configurationTopics);
        Map<String, TopicMapping.MappingEntry> topicMapping;
        try {
            topicMapping = getTopicMapping(configurationTopics);
        } catch (IllegalArgumentException e) {
            throw new InvalidConfigurationException("Malformed mqttTopicMapping", e);
        }
        return new BridgeConfig(brokerUri, clientId, topicMapping);
    }

    private static URI getBrokerUri(Topics configurationTopics) throws URISyntaxException {
        // brokerUri takes precedence since brokerServerUri is deprecated
        String brokerServerUri = Coerce.toString(
                configurationTopics.findOrDefault(DEFAULT_BROKER_URI, KEY_BROKER_SERVER_URI));
        String brokerUri = Coerce.toString(
                configurationTopics.findOrDefault(brokerServerUri, KEY_BROKER_URI));
        return new URI(brokerUri);
    }

    private static String getClientId(Topics topics) {
        return Coerce.toString(topics.findOrDefault(DEFAULT_CLIENT_ID, KEY_CLIENT_ID));
    }

    private static Map<String, TopicMapping.MappingEntry> getTopicMapping(Topics configurationTopics) {
        return OBJECT_MAPPER.convertValue(
                configurationTopics.lookupTopics(KEY_MQTT_TOPIC_MAPPING).toPOJO(),
                new TypeReference<Map<String, TopicMapping.MappingEntry>>() {
                });
    }

    public boolean reinstallRequired(BridgeConfig newConfig) {
        return !Objects.equals(getBrokerUri(), newConfig.getBrokerUri())
                || !Objects.equals(getClientId(), newConfig.getClientId());
    }
}
