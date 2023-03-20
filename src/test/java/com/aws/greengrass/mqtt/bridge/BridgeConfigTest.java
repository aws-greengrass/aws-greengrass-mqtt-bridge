/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge;

import com.aws.greengrass.componentmanager.KernelConfigResolver;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.dependency.Context;
import com.aws.greengrass.mqtt.bridge.model.InvalidConfigurationException;
import com.aws.greengrass.mqtt.bridge.model.Mqtt5RouteOptions;
import com.aws.greengrass.mqtt.bridge.model.MqttVersion;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.util.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
    private static final MqttVersion DEFAULT_MQTT_VERSION = MqttVersion.MQTT3;
    private static final int DEFAULT_RECEIVE_MAXIMUM = 65535;
    private static final Long DEFAULT_MAXIMUM_PACKET_SIZE = null;
    private static final long DEFAULT_SESSION_EXPIRY_INTERVAL = 4_294_967_295L;
    private static final String BROKER_URI = "tcp://localhost:8883";
    private static final String BROKER_SERVER_URI = "tcp://localhost:8884";
    private static final String MALFORMED_BROKER_URI = "tcp://ma]formed.uri:8883";
    private static final String CLIENT_ID = "clientId";

    private static final BridgeConfig BASE_CONFIG = BridgeConfig.builder()
            .brokerUri(URI.create(DEFAULT_BROKER_URI))
            .topicMapping(Collections.emptyMap())
            .mqtt5RouteOptions(Collections.emptyMap())
            .mqttVersion(DEFAULT_MQTT_VERSION)
            .receiveMaximum(DEFAULT_RECEIVE_MAXIMUM)
            .maximumPacketSize(DEFAULT_MAXIMUM_PACKET_SIZE)
            .sessionExpiryInterval(DEFAULT_SESSION_EXPIRY_INTERVAL)
            .build();

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
        BridgeConfig expectedConfig = BASE_CONFIG.toBuilder()
                .clientId(config.getClientId())
                .build();
        assertDefaultClientId(config);
        assertEquals(expectedConfig, config);
    }

    @Test
    void GIVEN_brokerUri_config_WHEN_bridge_config_created_THEN_uri_set() throws InvalidConfigurationException {
        topics.lookup(BridgeConfig.KEY_BROKER_URI).dflt(BROKER_URI);

        BridgeConfig config = BridgeConfig.fromTopics(topics);
        BridgeConfig expectedConfig = BASE_CONFIG.toBuilder()
                .brokerUri(URI.create(BROKER_URI))
                .clientId(config.getClientId())
                .build();
        assertDefaultClientId(config);
        assertEquals(expectedConfig, config);
    }

    @Test
    void GIVEN_brokerUri_and_brokerServerUri_config_WHEN_bridge_config_created_THEN_brokerUri_used() throws InvalidConfigurationException {
        topics.lookup(BridgeConfig.KEY_BROKER_SERVER_URI).dflt(BROKER_SERVER_URI);
        topics.lookup(BridgeConfig.KEY_BROKER_URI).dflt(BROKER_URI);

        BridgeConfig config = BridgeConfig.fromTopics(topics);
        BridgeConfig expectedConfig = BASE_CONFIG.toBuilder()
                .brokerUri(URI.create(BROKER_URI))
                .clientId(config.getClientId())
                .build();
        assertDefaultClientId(config);
        assertEquals(expectedConfig, config);
    }

    @Test
    void GIVEN_brokerServerUri_provided_WHEN_bridge_config_created_THEN_brokerServerUri_used() throws InvalidConfigurationException {
        topics.lookup(BridgeConfig.KEY_BROKER_SERVER_URI).dflt(BROKER_SERVER_URI);

        BridgeConfig config = BridgeConfig.fromTopics(topics);
        BridgeConfig expectedConfig = BASE_CONFIG.toBuilder()
                .brokerUri(URI.create(BROKER_SERVER_URI))
                .clientId(config.getClientId())
                .build();
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
        BridgeConfig expectedConfig = BASE_CONFIG.toBuilder()
                .clientId(CLIENT_ID)
                .build();
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
        BridgeConfig expectedConfig = BASE_CONFIG.toBuilder()
                .clientId(config.getClientId())
                .topicMapping(expectedEntries)
                .build();
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
        BridgeConfig expectedConfig = BASE_CONFIG.toBuilder()
                .clientId(config.getClientId())
                .mqttVersion(MqttVersion.MQTT5)
                .build();
        assertDefaultClientId(config);
        assertEquals(expectedConfig, config);
    }

    @Test
    void GIVEN_invalid_mqtt_version_config_WHEN_bridge_config_created_THEN_default_version_used() throws InvalidConfigurationException {
        topics.lookup(BridgeConfig.KEY_BROKER_CLIENT, BridgeConfig.KEY_VERSION).dflt("INVALID_VALUE");
        BridgeConfig config = BridgeConfig.fromTopics(topics);
        BridgeConfig expectedConfig = BASE_CONFIG.toBuilder()
                .clientId(config.getClientId())
                .build();
        assertDefaultClientId(config);
        assertEquals(expectedConfig, config);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 1234, DEFAULT_RECEIVE_MAXIMUM})
    void GIVEN_receiveMaximum_provided_WHEN_bridge_config_created_THEN_receiveMaximum_used(int receiveMaximum) throws InvalidConfigurationException {
        topics.lookup(BridgeConfig.KEY_BROKER_CLIENT, BridgeConfig.KEY_RECEIVE_MAXIMUM).dflt(receiveMaximum);

        BridgeConfig config = BridgeConfig.fromTopics(topics);
        BridgeConfig expectedConfig = BASE_CONFIG.toBuilder()
                .clientId(config.getClientId())
                .receiveMaximum(receiveMaximum)
                .build();
        assertDefaultClientId(config);
        assertEquals(expectedConfig, config);
    }

    @ParameterizedTest
    @ValueSource(ints = {DEFAULT_RECEIVE_MAXIMUM + 1, Integer.MAX_VALUE})
    void GIVEN_too_large_receiveMaximum_provided_WHEN_bridge_config_created_THEN_max_receiveMaximum_used(int invalidReceiveMaximum) throws InvalidConfigurationException {
        topics.lookup(BridgeConfig.KEY_BROKER_CLIENT, BridgeConfig.KEY_RECEIVE_MAXIMUM).dflt(invalidReceiveMaximum);

        BridgeConfig config = BridgeConfig.fromTopics(topics);
        BridgeConfig expectedConfig = BASE_CONFIG.toBuilder()
                .clientId(config.getClientId())
                .receiveMaximum(65535)
                .build();
        assertDefaultClientId(config);
        assertEquals(expectedConfig, config);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
    void GIVEN_too_small_receiveMaximum_provided_WHEN_bridge_config_created_THEN_min_receiveMaximum_used(int invalidReceiveMaximum) throws InvalidConfigurationException {
        topics.lookup(BridgeConfig.KEY_BROKER_CLIENT, BridgeConfig.KEY_RECEIVE_MAXIMUM).dflt(invalidReceiveMaximum);

        BridgeConfig config = BridgeConfig.fromTopics(topics);
        BridgeConfig expectedConfig = BASE_CONFIG.toBuilder()
                .clientId(config.getClientId())
                .receiveMaximum(1)
                .build();
        assertDefaultClientId(config);
        assertEquals(expectedConfig, config);
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 1234, 4294967295L})
    void GIVEN_maximumPacketSize_provided_WHEN_bridge_config_created_THEN_maximumPacketSize_used(long maximumPacketSize) throws InvalidConfigurationException {
        topics.lookup(BridgeConfig.KEY_BROKER_CLIENT, BridgeConfig.KEY_MAXIMUM_PACKET_SIZE).dflt(maximumPacketSize);

        BridgeConfig config = BridgeConfig.fromTopics(topics);
        BridgeConfig expectedConfig = BASE_CONFIG.toBuilder()
                .clientId(config.getClientId())
                .maximumPacketSize(maximumPacketSize)
                .build();
        assertDefaultClientId(config);
        assertEquals(expectedConfig, config);
    }

    @ParameterizedTest
    @ValueSource(longs = {0L, -1L, Long.MIN_VALUE})
    void GIVEN_too_small_maximumPacketSize_provided_WHEN_bridge_config_created_THEN_min_maximumPacketSize_used(long invalidMaximumPacketSize) throws InvalidConfigurationException {
        topics.lookup(BridgeConfig.KEY_BROKER_CLIENT, BridgeConfig.KEY_MAXIMUM_PACKET_SIZE).dflt(invalidMaximumPacketSize);

        BridgeConfig config = BridgeConfig.fromTopics(topics);
        BridgeConfig expectedConfig = BASE_CONFIG.toBuilder()
                .clientId(config.getClientId())
                .maximumPacketSize(1L)
                .build();
        assertDefaultClientId(config);
        assertEquals(expectedConfig, config);
    }

    @ParameterizedTest
    @ValueSource(longs = {4294967296L, Long.MAX_VALUE})
    void GIVEN_too_large_maximumPacketSize_provided_WHEN_bridge_config_created_THEN_max_maximumPacketSize_used(long invalidMaximumPacketSize) throws InvalidConfigurationException {
        topics.lookup(BridgeConfig.KEY_BROKER_CLIENT, BridgeConfig.KEY_MAXIMUM_PACKET_SIZE).dflt(invalidMaximumPacketSize);

        BridgeConfig config = BridgeConfig.fromTopics(topics);
        BridgeConfig expectedConfig = BASE_CONFIG.toBuilder()
                .clientId(config.getClientId())
                .maximumPacketSize(4294967295L)
                .build();
        assertDefaultClientId(config);
        assertEquals(expectedConfig, config);
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 1234, 4294967295L})
    void GIVEN_sessionExpiryInterval_provided_WHEN_bridge_config_created_THEN_sessionExpiryInterval_used(long sessionExpiryInterval) throws InvalidConfigurationException {
        topics.lookup(BridgeConfig.KEY_BROKER_CLIENT, BridgeConfig.KEY_SESSION_EXPIRY_INTERVAL).dflt(sessionExpiryInterval);

        BridgeConfig config = BridgeConfig.fromTopics(topics);
        BridgeConfig expectedConfig = BASE_CONFIG.toBuilder()
                .clientId(config.getClientId())
                .sessionExpiryInterval(sessionExpiryInterval)
                .build();
        assertDefaultClientId(config);
        assertEquals(expectedConfig, config);
    }

    @ParameterizedTest
    @ValueSource(longs = {-1L, Long.MIN_VALUE})
    void GIVEN_too_small_sessionExpiryInterval_provided_WHEN_bridge_config_created_THEN_min_sessionExpiryInterval_used(long invalidSessionExpiryInterval) throws InvalidConfigurationException {
        topics.lookup(BridgeConfig.KEY_BROKER_CLIENT, BridgeConfig.KEY_SESSION_EXPIRY_INTERVAL).dflt(invalidSessionExpiryInterval);

        BridgeConfig config = BridgeConfig.fromTopics(topics);
        BridgeConfig expectedConfig = BASE_CONFIG.toBuilder()
                .clientId(config.getClientId())
                .sessionExpiryInterval(0L)
                .build();
        assertDefaultClientId(config);
        assertEquals(expectedConfig, config);
    }

    @ParameterizedTest
    @ValueSource(longs = {4_294_967_296L, Long.MAX_VALUE})
    void GIVEN_too_large_sessionExpiryInterval_provided_WHEN_bridge_config_created_THEN_max_sessionExpiryInterval_used(long invalidSessionExpiryInterval) throws InvalidConfigurationException {
        topics.lookup(BridgeConfig.KEY_BROKER_CLIENT, BridgeConfig.KEY_SESSION_EXPIRY_INTERVAL).dflt(invalidSessionExpiryInterval);

        BridgeConfig config = BridgeConfig.fromTopics(topics);
        BridgeConfig expectedConfig = BASE_CONFIG.toBuilder()
                .clientId(config.getClientId())
                .sessionExpiryInterval(4_294_967_295L)
                .build();
        assertDefaultClientId(config);
        assertEquals(expectedConfig, config);
    }

    @Test
    void GIVEN_mqtt5_route_options_WHEN_bridge_config_created_THEN_mqtt5_route_options_used() throws InvalidConfigurationException {
        topics.lookupTopics(BridgeConfig.KEY_MQTT_5_ROUTE_OPTIONS).replaceAndWait(
                Utils.immutableMap(
                        "m1", Utils.immutableMap("noLocal", "true", "retainAsPublished",  "false"),
                        "m2", null,
                        "m3", Utils.immutableMap("noLocal", "true")));

        Map<String, Mqtt5RouteOptions> expectedEntries = new HashMap<>();
        expectedEntries.put("m1", Mqtt5RouteOptions.builder().noLocal(true).retainAsPublished(false).build());
        expectedEntries.put("m3", Mqtt5RouteOptions.builder().noLocal(true).retainAsPublished(true).build());

        BridgeConfig config = BridgeConfig.fromTopics(topics);
        BridgeConfig expectedConfig = BASE_CONFIG.toBuilder()
                .clientId(config.getClientId())
                .mqtt5RouteOptions(expectedEntries)
                .build();
        assertDefaultClientId(config);
        assertEquals(expectedConfig, config);
    }

    @Test
    void GIVEN_invalid_mqtt5_route_options_WHEN_bridge_config_created_THEN_exception_thrown() {
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

    private void assertDefaultClientId(BridgeConfig config) {
        assertTrue(config.getClientId().startsWith(DEFAULT_CLIENT_ID_PREFIX));
    }
}
