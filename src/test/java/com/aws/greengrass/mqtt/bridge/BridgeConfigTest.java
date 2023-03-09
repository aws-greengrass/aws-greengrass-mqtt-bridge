/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge;

import com.aws.greengrass.componentmanager.KernelConfigResolver;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.dependency.Context;
import com.aws.greengrass.mqtt.bridge.model.InvalidConfigurationException;
import com.aws.greengrass.mqtt.bridge.model.MqttVersion;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.util.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(GGExtension.class)
class BridgeConfigTest {

    private static final String DEFAULT_BROKER_URI = "ssl://localhost:8883";
    private static final String DEFAULT_CLIENT_ID_PREFIX = "mqtt-bridge-";
    private static final String DEFAULT_MQTT_VERSION = "mqtt3";
    private static final String BROKER_URI = "tcp://localhost:8883";
    private static final String BROKER_SERVER_URI = "tcp://localhost:8884";
    private static final String MALFORMED_BROKER_URI = "tcp://ma]formed.uri:8883";
    private static final String CLIENT_ID = "clientId";

    Topics topics;

    @BeforeEach
    void setUp() {
        topics = Topics.of(new Context(), KernelConfigResolver.CONFIGURATION_CONFIG_KEY, null);
    }

    @AfterEach
    void tearDown() {
        topics.getContext().shutdown();
    }

    @Test
    void GIVEN_empty_config_WHEN_bridge_config_created_THEN_defaults_used() throws InvalidConfigurationException {
        BridgeConfig config = BridgeConfig.fromTopics(topics);
        BridgeConfig expectedConfig = new BridgeConfig(
                URI.create(DEFAULT_BROKER_URI),
                config.getClientId(),
                Collections.emptyMap(),
                MqttVersion.fromName(DEFAULT_MQTT_VERSION)
        );
        assertDefaultClientId(config);
        assertEquals(expectedConfig, config);
    }

    @Test
    void GIVEN_brokerUri_config_WHEN_bridge_config_created_THEN_uri_set() throws InvalidConfigurationException {
        topics.lookup(BridgeConfig.KEY_BROKER_URI).dflt(BROKER_URI);

        BridgeConfig config = BridgeConfig.fromTopics(topics);
        BridgeConfig expectedConfig = new BridgeConfig(
                URI.create(BROKER_URI),
                config.getClientId(),
                Collections.emptyMap(),
                MqttVersion.fromName(DEFAULT_MQTT_VERSION)
        );
        assertDefaultClientId(config);
        assertEquals(expectedConfig, config);
    }

    @Test
    void GIVEN_brokerUri_and_brokerServerUri_config_WHEN_bridge_config_created_THEN_brokerUri_used() throws InvalidConfigurationException {
        topics.lookup(BridgeConfig.KEY_BROKER_SERVER_URI).dflt(BROKER_SERVER_URI);
        topics.lookup(BridgeConfig.KEY_BROKER_URI).dflt(BROKER_URI);

        BridgeConfig config = BridgeConfig.fromTopics(topics);
        BridgeConfig expectedConfig = new BridgeConfig(
                URI.create(BROKER_URI),
                config.getClientId(),
                Collections.emptyMap(),
                MqttVersion.fromName(DEFAULT_MQTT_VERSION)
        );
        assertDefaultClientId(config);
        assertEquals(expectedConfig, config);
    }

    @Test
    void GIVEN_brokerServerUri_provided_WHEN_bridge_config_created_THEN_brokerServerUri_used() throws InvalidConfigurationException {
        topics.lookup(BridgeConfig.KEY_BROKER_SERVER_URI).dflt(BROKER_SERVER_URI);

        BridgeConfig config = BridgeConfig.fromTopics(topics);
        BridgeConfig expectedConfig = new BridgeConfig(
                URI.create(BROKER_SERVER_URI),
                config.getClientId(),
                Collections.emptyMap(),
                MqttVersion.fromName(DEFAULT_MQTT_VERSION)
        );
        assertDefaultClientId(config);
        assertEquals(expectedConfig, config);
    }

    @Test
    void GIVEN_malformed_brokerUri_WHEN_bridge_config_created_THEN_exception_thrown() {
        topics.lookup(BridgeConfig.KEY_BROKER_URI).dflt(MALFORMED_BROKER_URI);
        assertThrows(InvalidConfigurationException.class, () -> BridgeConfig.fromTopics(topics));
    }

    @Test
    void GIVEN_clientId_config_WHEN_bridge_config_created_THEN_clientId_used() throws InvalidConfigurationException {
        topics.lookup(BridgeConfig.KEY_CLIENT_ID).dflt(CLIENT_ID);

        BridgeConfig config = BridgeConfig.fromTopics(topics);
        BridgeConfig expectedConfig = new BridgeConfig(
                URI.create(DEFAULT_BROKER_URI),
                CLIENT_ID,
                Collections.emptyMap(),
                MqttVersion.fromName(DEFAULT_MQTT_VERSION)
        );
        assertEquals(expectedConfig, config);
    }

    @Test
    void GIVEN_topic_mapping_WHEN_bridge_config_created_THEN_topic_mapping_used() throws InvalidConfigurationException {
        topics.lookupTopics(BridgeConfig.KEY_MQTT_TOPIC_MAPPING).replaceAndWait(
                Utils.immutableMap(
                        "m1",
                        Utils.immutableMap("topic", "mqtt/topic", "source", TopicMapping.TopicType.LocalMqtt.toString(), "target", TopicMapping.TopicType.IotCore.toString()),
                        "m2",
                        Utils.immutableMap("topic", "mqtt/topic2", "source", TopicMapping.TopicType.LocalMqtt.toString(), "target", TopicMapping.TopicType.Pubsub.toString()),
                        "m3",
                        Utils.immutableMap("topic", "mqtt/topic3", "source", TopicMapping.TopicType.LocalMqtt.toString(), "target", TopicMapping.TopicType.IotCore.toString())));

        Map<String, TopicMapping.MappingEntry> expectedEntries = new HashMap<>();
        expectedEntries.put("m1", new TopicMapping.MappingEntry("mqtt/topic", TopicMapping.TopicType.LocalMqtt, TopicMapping.TopicType.IotCore));
        expectedEntries.put("m2", new TopicMapping.MappingEntry("mqtt/topic2", TopicMapping.TopicType.LocalMqtt, TopicMapping.TopicType.Pubsub));
        expectedEntries.put("m3", new TopicMapping.MappingEntry("mqtt/topic3", TopicMapping.TopicType.LocalMqtt, TopicMapping.TopicType.IotCore));

        BridgeConfig config = BridgeConfig.fromTopics(topics);
        BridgeConfig expectedConfig = new BridgeConfig(
                URI.create(DEFAULT_BROKER_URI),
                config.getClientId(),
                expectedEntries,
                MqttVersion.fromName(DEFAULT_MQTT_VERSION)
        );
        assertDefaultClientId(config);
        assertEquals(expectedConfig, config);
    }

    @Test
    void GIVEN_invalid_topic_mapping_WHEN_bridge_config_created_THEN_exception_thrown() {
        String invalidSource = "INVALID_SOURCE";

        topics.lookupTopics(BridgeConfig.KEY_MQTT_TOPIC_MAPPING).replaceAndWait(
                Utils.immutableMap(
                        "m1",
                        Utils.immutableMap("topic", "mqtt/topic", "source", invalidSource, "target", TopicMapping.TopicType.IotCore.toString()),
                        "m2",
                        Utils.immutableMap("topic", "mqtt/topic2", "source", TopicMapping.TopicType.LocalMqtt.toString(), "target", TopicMapping.TopicType.Pubsub.toString()),
                        "m3",
                        Utils.immutableMap("topic", "mqtt/topic3", "source", TopicMapping.TopicType.LocalMqtt.toString(), "target", TopicMapping.TopicType.IotCore.toString())));

        assertThrows(InvalidConfigurationException.class, () -> BridgeConfig.fromTopics(topics));
    }

    @Test
    void GIVEN_mqtt_version_config_WHEN_bridge_config_created_THEN_version_set() throws InvalidConfigurationException {
        topics.lookup(BridgeConfig.KEY_BROKER_CLIENT, BridgeConfig.KEY_VERSION).dflt("mqtt5");

        BridgeConfig config = BridgeConfig.fromTopics(topics);
        BridgeConfig expectedConfig = new BridgeConfig(
                URI.create(DEFAULT_BROKER_URI),
                config.getClientId(),
                Collections.emptyMap(),
                MqttVersion.fromName("mqtt5")
        );
        assertDefaultClientId(config);
        assertEquals(expectedConfig, config);
    }

    @Test
    void GIVEN_invalid_mqtt_version_config_WHEN_bridge_config_created_THEN_default_version_used() throws InvalidConfigurationException {
        topics.lookup(BridgeConfig.KEY_BROKER_CLIENT, BridgeConfig.KEY_VERSION).dflt("INVALID_VALUE");
        BridgeConfig config = BridgeConfig.fromTopics(topics);
        BridgeConfig expectedConfig = new BridgeConfig(
                URI.create(DEFAULT_BROKER_URI),
                config.getClientId(),
                Collections.emptyMap(),
                MqttVersion.fromName(DEFAULT_MQTT_VERSION)
        );
        assertDefaultClientId(config);
        assertEquals(expectedConfig, config);
    }

    private void assertDefaultClientId(BridgeConfig config) {
        assertTrue(config.getClientId().startsWith(DEFAULT_CLIENT_ID_PREFIX));
    }
}
