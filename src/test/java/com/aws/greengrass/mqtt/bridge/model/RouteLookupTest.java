/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.model;

import com.aws.greengrass.mqtt.bridge.BridgeConfig;
import com.aws.greengrass.mqtt.bridge.TopicMapping;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class RouteLookupTest {

    @Mock
    BridgeConfig bridgeConfig;
    RouteLookup routeLookup;

    @BeforeEach
    void setUp() {
        routeLookup = new RouteLookup(new BridgeConfigReference(bridgeConfig));
    }

    @Test
    void GIVEN_route_config_iot_core_to_local_WHEN_noLocal_THEN_noLocal_returned() {
        when(bridgeConfig.getMqttVersion()).thenReturn(MqttVersion.MQTT5);

        Map<String, TopicMapping.MappingEntry> topicMapping = new HashMap<>();
        topicMapping.put("topic", new TopicMapping.MappingEntry("topic", TopicMapping.TopicType.IotCore, TopicMapping.TopicType.LocalMqtt));
        topicMapping.put("topic2", new TopicMapping.MappingEntry("topic2", TopicMapping.TopicType.IotCore, TopicMapping.TopicType.LocalMqtt));
        when(bridgeConfig.getTopicMapping()).thenReturn(topicMapping);

        Map<String, Mqtt5RouteOptions> routeOptions = new HashMap<>();
        routeOptions.put("topic", Mqtt5RouteOptions.builder().noLocal(true).build());
        when(bridgeConfig.getMqtt5RouteOptions()).thenReturn(routeOptions);

        assertTrue(routeLookup.noLocal("topic", TopicMapping.TopicType.IotCore).get());
        // routes not defined
        assertFalse(routeLookup.noLocal("topic2", TopicMapping.TopicType.IotCore).isPresent());
        assertFalse(routeLookup.noLocal("topic", TopicMapping.TopicType.LocalMqtt).isPresent());
        assertFalse(routeLookup.noLocal("topic", TopicMapping.TopicType.LocalMqtt).isPresent());
        assertFalse(routeLookup.noLocal("unknown", TopicMapping.TopicType.LocalMqtt).isPresent());
        
        routeOptions = new HashMap<>();
        routeOptions.put("topic", Mqtt5RouteOptions.builder().noLocal(false).build());
        when(bridgeConfig.getMqtt5RouteOptions()).thenReturn(routeOptions);

        assertFalse(routeLookup.noLocal("topic", TopicMapping.TopicType.IotCore).get());
        // routes not defined
        assertFalse(routeLookup.noLocal("topic2", TopicMapping.TopicType.IotCore).isPresent());
        assertFalse(routeLookup.noLocal("topic", TopicMapping.TopicType.LocalMqtt).isPresent());
        assertFalse(routeLookup.noLocal("topic", TopicMapping.TopicType.LocalMqtt).isPresent());
        assertFalse(routeLookup.noLocal("unknown", TopicMapping.TopicType.LocalMqtt).isPresent());
    }

    @Test
    void GIVEN_route_config_local_to_iotcore_WHEN_noLocal_THEN_noLocal_returned() {
        when(bridgeConfig.getMqttVersion()).thenReturn(MqttVersion.MQTT5);

        Map<String, TopicMapping.MappingEntry> topicMapping = new HashMap<>();
        topicMapping.put("topic", new TopicMapping.MappingEntry("topic", TopicMapping.TopicType.LocalMqtt, TopicMapping.TopicType.IotCore));
        topicMapping.put("topic2", new TopicMapping.MappingEntry("topic2", TopicMapping.TopicType.LocalMqtt, TopicMapping.TopicType.IotCore));
        when(bridgeConfig.getTopicMapping()).thenReturn(topicMapping);

        Map<String, Mqtt5RouteOptions> routeOptions = new HashMap<>();
        routeOptions.put("topic", Mqtt5RouteOptions.builder().noLocal(true).build());
        when(bridgeConfig.getMqtt5RouteOptions()).thenReturn(routeOptions);

        assertTrue(routeLookup.noLocal("topic", TopicMapping.TopicType.LocalMqtt).get());
        // routes not defined
        assertFalse(routeLookup.noLocal("topic2", TopicMapping.TopicType.LocalMqtt).isPresent());
        assertFalse(routeLookup.noLocal("topic", TopicMapping.TopicType.IotCore).isPresent());
        assertFalse(routeLookup.noLocal("topic", TopicMapping.TopicType.IotCore).isPresent());
        assertFalse(routeLookup.noLocal("unknown", TopicMapping.TopicType.LocalMqtt).isPresent());

        routeOptions = new HashMap<>();
        routeOptions.put("topic", Mqtt5RouteOptions.builder().noLocal(false).build());
        when(bridgeConfig.getMqtt5RouteOptions()).thenReturn(routeOptions);

        assertFalse(routeLookup.noLocal("topic", TopicMapping.TopicType.LocalMqtt).get());
        // routes not defined
        assertFalse(routeLookup.noLocal("topic2", TopicMapping.TopicType.LocalMqtt).isPresent());
        assertFalse(routeLookup.noLocal("topic", TopicMapping.TopicType.IotCore).isPresent());
        assertFalse(routeLookup.noLocal("topic", TopicMapping.TopicType.IotCore).isPresent());
        assertFalse(routeLookup.noLocal("unknown", TopicMapping.TopicType.LocalMqtt).isPresent());
    }

    @Test
    void GIVEN_route_config_and_mqtt_version_is_3_WHEN_noLocal_THEN_noLocal_not_returned() {
        when(bridgeConfig.getMqttVersion()).thenReturn(MqttVersion.MQTT3);

        Map<String, TopicMapping.MappingEntry> topicMapping = new HashMap<>();
        topicMapping.put("topic", new TopicMapping.MappingEntry("topic", TopicMapping.TopicType.LocalMqtt, TopicMapping.TopicType.IotCore));
        topicMapping.put("topic2", new TopicMapping.MappingEntry("topic2", TopicMapping.TopicType.IotCore, TopicMapping.TopicType.LocalMqtt));
        lenient().when(bridgeConfig.getTopicMapping()).thenReturn(topicMapping);

        Map<String, Mqtt5RouteOptions> routeOptions = new HashMap<>();
        routeOptions.put("topic", Mqtt5RouteOptions.builder().noLocal(true).build());
        routeOptions.put("topic2", Mqtt5RouteOptions.builder().noLocal(true).build());
        lenient().when(bridgeConfig.getMqtt5RouteOptions()).thenReturn(routeOptions);

        // routes not defined,
        // because noLocal is not valid for mqtt3
        assertFalse(routeLookup.noLocal("topic", TopicMapping.TopicType.LocalMqtt).isPresent());
        assertFalse(routeLookup.noLocal("topic2", TopicMapping.TopicType.LocalMqtt).isPresent());
        assertFalse(routeLookup.noLocal("topic", TopicMapping.TopicType.IotCore).isPresent());
        assertFalse(routeLookup.noLocal("topic", TopicMapping.TopicType.IotCore).isPresent());
        assertFalse(routeLookup.noLocal("unknown", TopicMapping.TopicType.LocalMqtt).isPresent());
    }
}
