/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge;

import com.aws.greengrass.mqtt.bridge.clients.MessageClient;
import com.aws.greengrass.mqtt.bridge.clients.MessageClientException;
import com.aws.greengrass.mqtt.bridge.clients.MessageClients;
import com.aws.greengrass.mqtt.bridge.model.BridgeConfigReference;
import com.aws.greengrass.mqtt.bridge.model.Message;
import com.aws.greengrass.mqtt.bridge.model.MqttMessage;
import com.aws.greengrass.mqtt.bridge.model.PubSubMessage;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.util.Utils;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class MessageBridgeTest {

    @Spy
    FakeLocalMessageClient localClient;
    @Spy
    FakePubSubMessageClient pubSubClient;
    @Spy
    FakeIotCoreMessageClient iotCoreClient;
    MessageClients messageClients;

    BridgeConfigReference bridgeConfig = new BridgeConfigReference();
    TopicMapping topicMapping = new TopicMapping();
    MessageBridge messageBridge;

    @BeforeEach
    void setUp() {
        bridgeConfig.set(BridgeConfig.builder().build());
        messageClients = new MessageClients(iotCoreClient, localClient, pubSubClient);
        messageBridge = new MessageBridge(topicMapping, bridgeConfig, messageClients);
    }

    @Test
    void GIVEN_topic_mapping_exists_WHEN_bridge_initialized_THEN_subscribed() throws MessageClientException {
        Map<String, TopicMapping.MappingEntry> mapping = Utils.immutableMap(
                "m1", new TopicMapping.MappingEntry(
                        "mqtt/topic", 
                        TopicMapping.TopicType.LocalMqtt, 
                        TopicMapping.TopicType.IotCore
                ), 
                "m2", new TopicMapping.MappingEntry(
                        "mqtt/topic2", 
                        TopicMapping.TopicType.LocalMqtt,
                        TopicMapping.TopicType.Pubsub
                ), 
                "m3", new TopicMapping.MappingEntry(
                        "mqtt/topic3", 
                        TopicMapping.TopicType.IotCore,
                        TopicMapping.TopicType.LocalMqtt
                ), 
                "m4", new TopicMapping.MappingEntry(
                        "mqtt/topic4", 
                        TopicMapping.TopicType.Pubsub,
                        TopicMapping.TopicType.LocalMqtt
                ));
        topicMapping.updateMapping(mapping);
        
        messageBridge.initialize();
        Set<String> localTopics = new HashSet<>(Arrays.asList("mqtt/topic", "mqtt/topic2"));
        assertEquals(localTopics, localClient.getSubscriptions());

        Set<String> pubSubTopics = Collections.singleton("mqtt/topic4");
        assertEquals(pubSubTopics, pubSubClient.getSubscriptions());

        Set<String> iotCoreTopics = Collections.singleton("mqtt/topic3");
        assertEquals(iotCoreTopics, iotCoreClient.getSubscriptions());
    }

    @Builder
    @Data
    private static class MappingChange {
        Map<String, TopicMapping.MappingEntry> mapping;
        Set<String> expectedLocalTopics;
        Set<String> expectedPubSubTopics;
        Set<String> expectedIotCoreTopics;
    }

    private static Stream<Arguments> mappingChanges() {
        return Stream.of(
                Arguments.of(Arrays.asList(
                        MappingChange.builder()
                                .mapping(Utils.immutableMap("m1",
                                        new TopicMapping.MappingEntry("mqtt/topic", TopicMapping.TopicType.LocalMqtt,
                                                TopicMapping.TopicType.IotCore), "m2",
                                        new TopicMapping.MappingEntry("mqtt/topic2", TopicMapping.TopicType.LocalMqtt,
                                                TopicMapping.TopicType.Pubsub), "m3",
                                        new TopicMapping.MappingEntry("mqtt/topic3", TopicMapping.TopicType.IotCore,
                                                TopicMapping.TopicType.LocalMqtt), "m4",
                                        new TopicMapping.MappingEntry("mqtt/topic4", TopicMapping.TopicType.Pubsub,
                                                TopicMapping.TopicType.LocalMqtt)))
                                .expectedLocalTopics(new HashSet<>(Arrays.asList("mqtt/topic", "mqtt/topic2")))
                                .expectedIotCoreTopics(Collections.singleton("mqtt/topic3"))
                                .expectedPubSubTopics(Collections.singleton("mqtt/topic4"))
                                .build(),

                        MappingChange.builder()
                                .mapping(Utils.immutableMap("m1",
                                        new TopicMapping.MappingEntry("mqtt/topic", TopicMapping.TopicType.LocalMqtt,
                                                TopicMapping.TopicType.IotCore), "m2",
                                        new TopicMapping.MappingEntry("mqtt/topic2/changed", TopicMapping.TopicType.LocalMqtt,
                                                TopicMapping.TopicType.Pubsub), "m3",
                                        new TopicMapping.MappingEntry("mqtt/topic3/added", TopicMapping.TopicType.LocalMqtt,
                                                TopicMapping.TopicType.Pubsub), "m4",
                                        new TopicMapping.MappingEntry("mqtt/topic3", TopicMapping.TopicType.Pubsub,
                                                TopicMapping.TopicType.LocalMqtt)))
                                .expectedLocalTopics(new HashSet<>(Arrays.asList("mqtt/topic", "mqtt/topic2/changed", "mqtt/topic3/added")))
                                .expectedIotCoreTopics(Collections.emptySet())
                                .expectedPubSubTopics(Collections.singleton("mqtt/topic3"))
                                .build(),

                        MappingChange.builder()
                                .mapping(Collections.emptyMap())
                                .expectedLocalTopics(Collections.emptySet())
                                .expectedIotCoreTopics(Collections.emptySet())
                                .expectedPubSubTopics(Collections.emptySet())
                                .build()
                ))
        );
    }

    @ParameterizedTest
    @MethodSource("mappingChanges")
    void GIVEN_bridge_WHEN_mapping_updated_THEN_subscriptions_updated(List<MappingChange> mappingChanges) throws MessageClientException {
        messageBridge.initialize();
        mappingChanges.forEach(change -> {
            topicMapping.updateMapping(change.getMapping());
            assertEquals(change.getExpectedLocalTopics(), localClient.getSubscriptions());
            assertEquals(change.getExpectedIotCoreTopics(), iotCoreClient.getSubscriptions());
            assertEquals(change.getExpectedPubSubTopics(), pubSubClient.getSubscriptions());
        });
    }


//    @Test
//    @SuppressWarnings("unchecked")
//    void GIVEN_mqtt_bridge_and_mapping_populated_WHEN_receive_mqtt_message_THEN_routed_to_iotcore_pubsub()
//            throws Exception {
//        TopicMapping mapping = new TopicMapping();
//        Map<String, TopicMapping.MappingEntry> mappingToUpdate = Utils.immutableMap("m1",
//                new TopicMapping.MappingEntry("mqtt/topic", TopicMapping.TopicType.LocalMqtt,
//                        TopicMapping.TopicType.IotCore), "m1-1",
//                new TopicMapping.MappingEntry("mqtt/topic", TopicMapping.TopicType.LocalMqtt,
//                        TopicMapping.TopicType.Pubsub), "m2",
//                new TopicMapping.MappingEntry("mqtt/topic2", TopicMapping.TopicType.LocalMqtt,
//                        TopicMapping.TopicType.Pubsub), "m3",
//                new TopicMapping.MappingEntry("mqtt/topic3", TopicMapping.TopicType.IotCore,
//                        TopicMapping.TopicType.LocalMqtt), "m4",
//                new TopicMapping.MappingEntry("mqtt/topic4", TopicMapping.TopicType.Pubsub,
//                        TopicMapping.TopicType.LocalMqtt));
//        mapping.updateMapping(mappingToUpdate);
//
//        MessageBridge messageBridge = new MessageBridge(mapping, Collections.emptyMap());
//
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.LocalMqtt, localClient);
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.Pubsub, pubSubClient);
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.IotCore, iotCoreClient);
//
//        doReturn(true).when(localClient).supportsTopicFilters();
//
//        ArgumentCaptor<Consumer<MqttMessage>> messageHandlerLocalMqttCaptor = ArgumentCaptor.forClass(Consumer.class);
//        verify(localClient, times(1)).updateSubscriptions(any(), messageHandlerLocalMqttCaptor.capture());
//        ArgumentCaptor<Consumer<PubSubMessage>> messageHandlerPubSubCaptor = ArgumentCaptor.forClass(Consumer.class);
//        verify(pubSubClient, times(1)).updateSubscriptions(any(), messageHandlerPubSubCaptor.capture());
//        ArgumentCaptor<Consumer<MqttMessage>> messageHandlerIotCoreCaptor = ArgumentCaptor.forClass(Consumer.class);
//        verify(iotCoreClient, times(1)).updateSubscriptions(any(), messageHandlerIotCoreCaptor.capture());
//
//        //Make mockMessageClient3 throw. (Will be ignored)
//        doThrow(new MessageClientException("")).when(iotCoreClient).publish(any());
//        byte[] messageOnTopic1 = "message from topic mqtt/topic".getBytes();
//        byte[] messageOnTopic2 = "message from topic mqtt/topic2".getBytes();
//        messageHandlerLocalMqttCaptor.getValue().accept(MqttMessage.builder().topic("mqtt/topic").payload(messageOnTopic1).build());
//        messageHandlerLocalMqttCaptor.getValue().accept(MqttMessage.builder().topic("mqtt/topic2").payload(messageOnTopic2).build());
//
//        // Also send on an unknown topic
//        messageHandlerLocalMqttCaptor.getValue().accept(MqttMessage.builder().topic("mqtt/unknown").payload(messageOnTopic2).build());
//
//        verify(localClient, times(0)).publish(any());
//        ArgumentCaptor<PubSubMessage> messagePubSubCaptor = ArgumentCaptor.forClass(PubSubMessage.class);
//        verify(pubSubClient, times(2)).publish(messagePubSubCaptor.capture());
//        ArgumentCaptor<MqttMessage> messageIotCoreCaptor = ArgumentCaptor.forClass(MqttMessage.class);
//        verify(iotCoreClient, times(1)).publish(messageIotCoreCaptor.capture());
//
//        assertThat(messageIotCoreCaptor.getAllValues().get(0).getTopic(),
//                Matchers.is(Matchers.equalTo("mqtt/topic")));
//        Assertions.assertArrayEquals(messageOnTopic1, messageIotCoreCaptor.getAllValues().get(0).getPayload());
//
//        assertThat(messagePubSubCaptor.getAllValues().get(0).getTopic(),
//                Matchers.is(Matchers.equalTo("mqtt/topic")));
//        Assertions.assertArrayEquals(messageOnTopic1, messagePubSubCaptor.getAllValues().get(0).getPayload());
//        assertThat(messagePubSubCaptor.getAllValues().get(1).getTopic(),
//                Matchers.is(Matchers.equalTo("mqtt/topic2")));
//        Assertions.assertArrayEquals(messageOnTopic2, messagePubSubCaptor.getAllValues().get(1).getPayload());
//    }
//
//    @Test
//    @SuppressWarnings("unchecked")
//    void GIVEN_mqtt_bridge_and_mapping_populated_with_filters_WHEN_receive_mqtt_message_THEN_routed_correctly()
//            throws Exception {
//        TopicMapping mapping = new TopicMapping();
//        Map<String, TopicMapping.MappingEntry> mappingToUpdate = Utils.immutableMap("m1",
//                new TopicMapping.MappingEntry("sensors/+/humidity", TopicMapping.TopicType.LocalMqtt,
//                        TopicMapping.TopicType.IotCore), "m2",
//                new TopicMapping.MappingEntry("sensors/satellite/#", TopicMapping.TopicType.IotCore,
//                        TopicMapping.TopicType.Pubsub), "m3",
//                new TopicMapping.MappingEntry("sensors/satellite/altitude", TopicMapping.TopicType.IotCore,
//                        TopicMapping.TopicType.LocalMqtt), "m4",
//                new TopicMapping.MappingEntry("sensors/thermostat1/humidity", TopicMapping.TopicType.LocalMqtt,
//                        TopicMapping.TopicType.Pubsub), "m5",
//                // This will cause a duplicate message to IoTCore
//                // (one for sensors/+/humidity)
//                new TopicMapping.MappingEntry("sensors/thermostat1/humidity", TopicMapping.TopicType.LocalMqtt,
//                        TopicMapping.TopicType.IotCore));
//        mapping.updateMapping(mappingToUpdate);
//
//        MessageBridge messageBridge = new MessageBridge(mapping, Collections.emptyMap());
//
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.LocalMqtt, localClient);
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.Pubsub, pubSubClient);
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.IotCore, iotCoreClient);
//
//        doReturn(true).when(localClient).supportsTopicFilters();
//        doReturn(true).when(iotCoreClient).supportsTopicFilters();
//        ArgumentCaptor<Consumer<MqttMessage>> messageHandlerLocalMqttCaptor = ArgumentCaptor.forClass(Consumer.class);
//        verify(localClient, times(1)).updateSubscriptions(any(), messageHandlerLocalMqttCaptor.capture());
//        ArgumentCaptor<Consumer<PubSubMessage>> messageHandlerPubSubCaptor = ArgumentCaptor.forClass(Consumer.class);
//        verify(pubSubClient, times(1)).updateSubscriptions(any(), messageHandlerPubSubCaptor.capture());
//        ArgumentCaptor<Consumer<MqttMessage>> messageHandlerIotCoreCaptor = ArgumentCaptor.forClass(Consumer.class);
//        verify(iotCoreClient, times(1)).updateSubscriptions(any(), messageHandlerIotCoreCaptor.capture());
//
//        byte[] messageFromThermostat1 = "humidity = 40%".getBytes();
//        byte[] messageFromThermostat2 = "humidity = 41%".getBytes();
//        byte[] messageFromThermostat2Temp = "temperature = 70C".getBytes();
//        messageHandlerLocalMqttCaptor.getValue()
//                .accept(MqttMessage.builder().topic("sensors/thermostat1/humidity").payload(messageFromThermostat1).build());
//        messageHandlerLocalMqttCaptor.getValue()
//                .accept(MqttMessage.builder().topic("sensors/thermostat2/humidity").payload(messageFromThermostat2).build());
//
//        // Also send for an unknown measurement
//        messageHandlerLocalMqttCaptor.getValue()
//                .accept(MqttMessage.builder().topic("sensors/thermostat1/temperature").payload(messageFromThermostat2Temp).build());
//        // Also send for a topic with multiple nodes to match with the filter (which should not match)
//        messageHandlerLocalMqttCaptor.getValue()
//                .accept(MqttMessage.builder().topic("sensors/thermostat2/zone1/humidity").payload(messageFromThermostat2).build());
//
//        verify(localClient, times(0)).publish(any());
//        ArgumentCaptor<PubSubMessage> messagePubSubCaptor = ArgumentCaptor.forClass(PubSubMessage.class);
//        verify(pubSubClient, times(1)).publish(messagePubSubCaptor.capture());
//        ArgumentCaptor<MqttMessage> messageIotCoreCaptor = ArgumentCaptor.forClass(MqttMessage.class);
//        verify(iotCoreClient, times(3)).publish(messageIotCoreCaptor.capture());
//
//        assertThat(messagePubSubCaptor.getAllValues().get(0).getTopic(),
//                Matchers.is(Matchers.equalTo("sensors/thermostat1/humidity")));
//        Assertions.assertArrayEquals(messageFromThermostat1, messagePubSubCaptor.getAllValues().get(0).getPayload());
//
//        assertThat(messageIotCoreCaptor.getAllValues().get(0).getTopic(),
//                Matchers.is(Matchers.equalTo("sensors/thermostat1/humidity")));
//        Assertions.assertArrayEquals(messageFromThermostat1, messageIotCoreCaptor.getAllValues().get(0).getPayload());
//        assertThat(messageIotCoreCaptor.getAllValues().get(1).getTopic(),
//                Matchers.is(Matchers.equalTo("sensors/thermostat1/humidity")));
//        Assertions.assertArrayEquals(messageFromThermostat1, messageIotCoreCaptor.getAllValues().get(1).getPayload());
//        assertThat(messageIotCoreCaptor.getAllValues().get(2).getTopic(),
//                Matchers.is(Matchers.equalTo("sensors/thermostat2/humidity")));
//        Assertions.assertArrayEquals(messageFromThermostat2, messageIotCoreCaptor.getAllValues().get(2).getPayload());
//
//        byte[] messageFromSatelliteForAltitude = "altitude = 10000".getBytes();
//        byte[] messageFromSatelliteForConnectivity = "conn = 41%".getBytes();
//        byte[] messageFromSatelliteForMultiLevel = "conn = 21%".getBytes();
//        messageHandlerIotCoreCaptor.getValue()
//                .accept(MqttMessage.builder().topic("sensors/satellite/altitude").payload(messageFromSatelliteForAltitude).build());
//        messageHandlerIotCoreCaptor.getValue()
//                .accept(MqttMessage.builder().topic("sensors/satellite/connectivity").payload(messageFromSatelliteForConnectivity).build());
//        messageHandlerIotCoreCaptor.getValue()
//                .accept(MqttMessage.builder().topic("sensors/satellite/device1/connectivity").payload(messageFromSatelliteForMultiLevel).build());
//
//        ArgumentCaptor<MqttMessage> messageLocalMqttCaptor = ArgumentCaptor.forClass(MqttMessage.class);
//        verify(localClient, times(1)).publish(messageLocalMqttCaptor.capture());
//        messagePubSubCaptor = ArgumentCaptor.forClass(PubSubMessage.class);
//        verify(pubSubClient, times(1 + 3)).publish(messagePubSubCaptor.capture());
//        messageIotCoreCaptor = ArgumentCaptor.forClass(MqttMessage.class);
//        verify(iotCoreClient, times(3)).publish(messageIotCoreCaptor.capture());
//
//        assertThat(messageLocalMqttCaptor.getAllValues().get(0).getTopic(),
//                Matchers.is(Matchers.equalTo("sensors/satellite/altitude")));
//        Assertions.assertArrayEquals(messageFromSatelliteForAltitude,
//                messageLocalMqttCaptor.getAllValues().get(0).getPayload());
//
//        assertThat(messagePubSubCaptor.getAllValues().get(1).getTopic(),
//                Matchers.is(Matchers.equalTo("sensors/satellite/altitude")));
//        Assertions.assertArrayEquals(messageFromSatelliteForAltitude,
//                messagePubSubCaptor.getAllValues().get(1).getPayload());
//        assertThat(messagePubSubCaptor.getAllValues().get(2).getTopic(),
//                Matchers.is(Matchers.equalTo("sensors/satellite/connectivity")));
//        Assertions.assertArrayEquals(messageFromSatelliteForConnectivity,
//                messagePubSubCaptor.getAllValues().get(2).getPayload());
//        assertThat(messagePubSubCaptor.getAllValues().get(3).getTopic(),
//                Matchers.is(Matchers.equalTo("sensors/satellite/device1/connectivity")));
//        Assertions.assertArrayEquals(messageFromSatelliteForMultiLevel,
//                messagePubSubCaptor.getAllValues().get(3).getPayload());
//    }
//
//    @Test
//    @SuppressWarnings("unchecked")
//    void GIVEN_mqtt_bridge_and_mapping_populated_with_filters_and_target_prefix_WHEN_receive_mqtt_message_THEN_routed_correctly()
//            throws Exception {
//        TopicMapping mapping = new TopicMapping();
//        Map<String, TopicMapping.MappingEntry> mappingToUpdate = Utils.immutableMap("m1",
//                new TopicMapping.MappingEntry("sensors/+/humidity", TopicMapping.TopicType.LocalMqtt,
//                        TopicMapping.TopicType.IotCore, "external/"), "m2",
//                new TopicMapping.MappingEntry("sensors/satellite/#", TopicMapping.TopicType.IotCore,
//                        TopicMapping.TopicType.Pubsub, "external/"), "m3",
//                new TopicMapping.MappingEntry("sensors/satellite/altitude", TopicMapping.TopicType.IotCore,
//                        TopicMapping.TopicType.LocalMqtt, "external/"), "m4",
//                new TopicMapping.MappingEntry("sensors/thermostat1/humidity", TopicMapping.TopicType.LocalMqtt,
//                        TopicMapping.TopicType.Pubsub, "external/"), "m5",
//                // This will cause a duplicate message to IoTCore
//                // (one for sensors/+/humidity)
//                new TopicMapping.MappingEntry("sensors/thermostat1/humidity", TopicMapping.TopicType.LocalMqtt,
//                        TopicMapping.TopicType.IotCore, "external/"));
//        mapping.updateMapping(mappingToUpdate);
//
//        MessageBridge messageBridge = new MessageBridge(mapping, Collections.emptyMap());
//
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.LocalMqtt, localClient);
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.Pubsub, pubSubClient);
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.IotCore, iotCoreClient);
//
//        doReturn(true).when(localClient).supportsTopicFilters();
//        doReturn(true).when(iotCoreClient).supportsTopicFilters();
//
//        ArgumentCaptor<Consumer<MqttMessage>> messageHandlerLocalMqttCaptor = ArgumentCaptor.forClass(Consumer.class);
//        verify(localClient, times(1)).updateSubscriptions(any(), messageHandlerLocalMqttCaptor.capture());
//        ArgumentCaptor<Consumer<PubSubMessage>> messageHandlerPubSubCaptor = ArgumentCaptor.forClass(Consumer.class);
//        verify(pubSubClient, times(1)).updateSubscriptions(any(), messageHandlerPubSubCaptor.capture());
//        ArgumentCaptor<Consumer<MqttMessage>> messageHandlerIotCoreCaptor = ArgumentCaptor.forClass(Consumer.class);
//        verify(iotCoreClient, times(1)).updateSubscriptions(any(), messageHandlerIotCoreCaptor.capture());
//
//        byte[] messageFromThermostat1 = "humidity = 40%".getBytes();
//        byte[] messageFromThermostat2 = "humidity = 41%".getBytes();
//        byte[] messageFromThermostat2Temp = "temperature = 70C".getBytes();
//        messageHandlerLocalMqttCaptor.getValue()
//                .accept(MqttMessage.builder().topic("sensors/thermostat1/humidity").payload(messageFromThermostat1).build());
//        messageHandlerLocalMqttCaptor.getValue()
//                .accept(MqttMessage.builder().topic("sensors/thermostat2/humidity").payload(messageFromThermostat2).build());
//
//        // Also send for an unknown measurement
//        messageHandlerLocalMqttCaptor.getValue()
//                .accept(MqttMessage.builder().topic("sensors/thermostat1/temperature").payload(messageFromThermostat2Temp).build());
//        // Also send for a topic with multiple nodes to match with the filter (which should not match)
//        messageHandlerLocalMqttCaptor.getValue()
//                .accept(MqttMessage.builder().topic("sensors/thermostat2/zone1/humidity").payload(messageFromThermostat2).build());
//
//        verify(localClient, times(0)).publish(any());
//        ArgumentCaptor<PubSubMessage> messagePubSubCaptor = ArgumentCaptor.forClass(PubSubMessage.class);
//        verify(pubSubClient, times(1)).publish(messagePubSubCaptor.capture());
//        ArgumentCaptor<MqttMessage> messageIotCoreCaptor = ArgumentCaptor.forClass(MqttMessage.class);
//        verify(iotCoreClient, times(3)).publish(messageIotCoreCaptor.capture());
//
//        assertThat(messagePubSubCaptor.getAllValues().get(0).getTopic(),
//                Matchers.is(Matchers.equalTo("external/sensors/thermostat1/humidity")));
//        Assertions.assertArrayEquals(messageFromThermostat1, messagePubSubCaptor.getAllValues().get(0).getPayload());
//
//        assertThat(messageIotCoreCaptor.getAllValues().get(0).getTopic(),
//                Matchers.is(Matchers.equalTo("external/sensors/thermostat1/humidity")));
//        Assertions.assertArrayEquals(messageFromThermostat1, messageIotCoreCaptor.getAllValues().get(0).getPayload());
//        assertThat(messageIotCoreCaptor.getAllValues().get(1).getTopic(),
//                Matchers.is(Matchers.equalTo("external/sensors/thermostat1/humidity")));
//        Assertions.assertArrayEquals(messageFromThermostat1, messageIotCoreCaptor.getAllValues().get(1).getPayload());
//        assertThat(messageIotCoreCaptor.getAllValues().get(2).getTopic(),
//                Matchers.is(Matchers.equalTo("external/sensors/thermostat2/humidity")));
//        Assertions.assertArrayEquals(messageFromThermostat2, messageIotCoreCaptor.getAllValues().get(2).getPayload());
//
//        byte[] messageFromSatelliteForAltitude = "altitude = 10000".getBytes();
//        byte[] messageFromSatelliteForConnectivity = "conn = 41%".getBytes();
//        byte[] messageFromSatelliteForMultiLevel = "conn = 21%".getBytes();
//        messageHandlerIotCoreCaptor.getValue()
//                .accept(MqttMessage.builder().topic("sensors/satellite/altitude").payload(messageFromSatelliteForAltitude).build());
//        messageHandlerIotCoreCaptor.getValue()
//                .accept(MqttMessage.builder().topic("sensors/satellite/connectivity").payload(messageFromSatelliteForConnectivity).build());
//        messageHandlerIotCoreCaptor.getValue()
//                .accept(MqttMessage.builder().topic("sensors/satellite/device1/connectivity").payload(messageFromSatelliteForMultiLevel).build());
//
//        ArgumentCaptor<MqttMessage> messageLocalMqttCaptor = ArgumentCaptor.forClass(MqttMessage.class);
//        verify(localClient, times(1)).publish(messageLocalMqttCaptor.capture());
//        messagePubSubCaptor = ArgumentCaptor.forClass(PubSubMessage.class);
//        verify(pubSubClient, times(1 + 3)).publish(messagePubSubCaptor.capture());
//        messageIotCoreCaptor = ArgumentCaptor.forClass(MqttMessage.class);
//        verify(iotCoreClient, times(3)).publish(messageIotCoreCaptor.capture());
//
//        assertThat(messageLocalMqttCaptor.getAllValues().get(0).getTopic(),
//                Matchers.is(Matchers.equalTo("external/sensors/satellite/altitude")));
//        Assertions.assertArrayEquals(messageFromSatelliteForAltitude,
//                messageLocalMqttCaptor.getAllValues().get(0).getPayload());
//
//        assertThat(messagePubSubCaptor.getAllValues().get(1).getTopic(),
//                Matchers.is(Matchers.equalTo("external/sensors/satellite/altitude")));
//        Assertions.assertArrayEquals(messageFromSatelliteForAltitude,
//                messagePubSubCaptor.getAllValues().get(1).getPayload());
//        assertThat(messagePubSubCaptor.getAllValues().get(2).getTopic(),
//                Matchers.is(Matchers.equalTo("external/sensors/satellite/connectivity")));
//        Assertions.assertArrayEquals(messageFromSatelliteForConnectivity,
//                messagePubSubCaptor.getAllValues().get(2).getPayload());
//        assertThat(messagePubSubCaptor.getAllValues().get(3).getTopic(),
//                Matchers.is(Matchers.equalTo("external/sensors/satellite/device1/connectivity")));
//        Assertions.assertArrayEquals(messageFromSatelliteForMultiLevel,
//                messagePubSubCaptor.getAllValues().get(3).getPayload());
//    }
//
//    @Test
//    @SuppressWarnings("unchecked")
//    void GIVEN_mqtt_bridge_and_mapping_populated_with_filters_in_pubsub_WHEN_receive_mqtt_message_THEN_routed_correctly()
//            throws Exception {
//        TopicMapping mapping = new TopicMapping();
//        Map<String, TopicMapping.MappingEntry> mappingToUpdate = Utils.immutableMap("m1",
//                new TopicMapping.MappingEntry("sensors/+/humidity", TopicMapping.TopicType.Pubsub,
//                        TopicMapping.TopicType.IotCore), "m2",
//                new TopicMapping.MappingEntry("sensors/thermostat1/humidity", TopicMapping.TopicType.Pubsub,
//                        TopicMapping.TopicType.IotCore));
//        mapping.updateMapping(mappingToUpdate);
//
//        MessageBridge messageBridge = new MessageBridge(mapping, Collections.emptyMap());
//
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.LocalMqtt, localClient);
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.Pubsub, pubSubClient);
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.IotCore, iotCoreClient);
//
//        ArgumentCaptor<Consumer<MqttMessage>> messageHandlerLocalMqttCaptor = ArgumentCaptor.forClass(Consumer.class);
//        verify(localClient, times(1)).updateSubscriptions(any(), messageHandlerLocalMqttCaptor.capture());
//        ArgumentCaptor<Consumer<PubSubMessage>> messageHandlerPubSubCaptor = ArgumentCaptor.forClass(Consumer.class);
//        verify(pubSubClient, times(1)).updateSubscriptions(any(), messageHandlerPubSubCaptor.capture());
//        ArgumentCaptor<Consumer<MqttMessage>> messageHandlerIotCoreCaptor = ArgumentCaptor.forClass(Consumer.class);
//        verify(iotCoreClient, times(1)).updateSubscriptions(any(), messageHandlerIotCoreCaptor.capture());
//
//        byte[] messageFromThermostat1 = "humidity = 40%".getBytes();
//        messageHandlerPubSubCaptor.getValue()
//                .accept(new PubSubMessage("sensors/thermostat1/humidity", messageFromThermostat1));
//
//        verify(localClient, times(0)).publish(any());
//        verify(pubSubClient, times(0)).publish(any());
//        ArgumentCaptor<MqttMessage> messageIotCoreCaptor = ArgumentCaptor.forClass(MqttMessage.class);
//        verify(iotCoreClient, times(2)).publish(messageIotCoreCaptor.capture());
//
//        assertThat(messageIotCoreCaptor.getAllValues().get(0).getTopic(),
//                Matchers.is(Matchers.equalTo("sensors/thermostat1/humidity")));
//        Assertions.assertArrayEquals(messageFromThermostat1, messageIotCoreCaptor.getAllValues().get(0).getPayload());
//    }
//
//    @Test
//    void GIVEN_mqtt_bridge_and_mqtt5_route_options_WHEN_retain_as_published_THEN_message_bridged_with_retain_flag()
//            throws MessageClientException {
//        TopicMapping mapping = new TopicMapping();
//        Map<String, TopicMapping.MappingEntry> mappingToUpdate = Utils.immutableMap("toLocal",
//                new TopicMapping.MappingEntry("topics/toLocal", TopicMapping.TopicType.LocalMqtt,
//                        TopicMapping.TopicType.LocalMqtt));
//        mapping.updateMapping(mappingToUpdate);
//
//        Map<String, Mqtt5RouteOptions> options = new HashMap<>();
//        options.put("topics/toLocal", Mqtt5RouteOptions.builder().retainAsPublished(true).build());
//        byte[] payload = "message".getBytes();
//
//        MessageBridge messageBridge = new MessageBridge(mapping, options);
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.LocalMqtt,
//                localClient);
//        doReturn(true).when(localClient).supportsTopicFilters();
//        ArgumentCaptor<Consumer<MqttMessage>> messageHandlerLocalMqttCaptor = ArgumentCaptor.forClass(Consumer.class);
//        verify(localClient, times(1)).updateSubscriptions(any(), messageHandlerLocalMqttCaptor.capture());
//        messageHandlerLocalMqttCaptor.getValue()
//                .accept(MqttMessage.builder().topic("topics/toLocal").payload(payload).retain(true).build());
//
//        ArgumentCaptor<MqttMessage> messageLocalMqttCaptor = ArgumentCaptor.forClass(MqttMessage.class);
//        verify(localClient, times(1)).publish(messageLocalMqttCaptor.capture());
//
//        assertThat(messageLocalMqttCaptor.getAllValues().get(0).getTopic(),
//                Matchers.is(Matchers.equalTo("topics/toLocal")));
//        Assertions.assertArrayEquals(payload, messageLocalMqttCaptor.getAllValues().get(0).getPayload());
//        assertEquals(true, messageLocalMqttCaptor.getAllValues().get(0).isRetain());
//    }
//
//    @Test
//    void GIVEN_mqtt_bridge_and_mqtt5_route_options_WHEN_retain_as_published_false_THEN_message_bridged_without_retain_flag()
//            throws MessageClientException {
//        TopicMapping mapping = new TopicMapping();
//        Map<String, TopicMapping.MappingEntry> mappingToUpdate = Utils.immutableMap("toLocal",
//                new TopicMapping.MappingEntry("topics/toLocal", TopicMapping.TopicType.LocalMqtt,
//                        TopicMapping.TopicType.LocalMqtt));
//        mapping.updateMapping(mappingToUpdate);
//
//        Map<String, Mqtt5RouteOptions> options = new HashMap<>();
//        options.put("topics/toLocal", Mqtt5RouteOptions.builder().retainAsPublished(false).build());
//        byte[] payload = "message".getBytes();
//
//        MessageBridge messageBridge = new MessageBridge(mapping, options);
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.LocalMqtt,
//                localClient);
//        doReturn(true).when(localClient).supportsTopicFilters();
//        ArgumentCaptor<Consumer<MqttMessage>> messageHandlerLocalMqttCaptor = ArgumentCaptor.forClass(Consumer.class);
//        verify(localClient, times(1)).updateSubscriptions(any(), messageHandlerLocalMqttCaptor.capture());
//        messageHandlerLocalMqttCaptor.getValue()
//                .accept(MqttMessage.builder().topic("topics/toLocal").payload(payload).retain(true).build());
//
//        ArgumentCaptor<MqttMessage> messageLocalMqttCaptor = ArgumentCaptor.forClass(MqttMessage.class);
//        verify(localClient, times(1)).publish(messageLocalMqttCaptor.capture());
//
//        assertThat(messageLocalMqttCaptor.getAllValues().get(0).getTopic(),
//                Matchers.is(Matchers.equalTo("topics/toLocal")));
//        Assertions.assertArrayEquals(payload, messageLocalMqttCaptor.getAllValues().get(0).getPayload());
//        assertEquals(false, messageLocalMqttCaptor.getAllValues().get(0).isRetain());
//    }

    static class FakeLocalMessageClient implements MessageClient<MqttMessage> {

        @Getter
        Set<String> subscriptions;
        
        @Override
        public void publish(MqttMessage message) throws MessageClientException {
        }

        @Override
        public void updateSubscriptions(Set<String> topics, Consumer<MqttMessage> messageHandler) {
            subscriptions = topics;
        }

        @Override
        public boolean supportsTopicFilters() {
            return true;
        }

        @Override
        public MqttMessage convertMessage(Message message) {
            return (MqttMessage) message.toMqtt();
        }

        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }

        @Override
        public TopicMapping.TopicType getType() {
            return TopicMapping.TopicType.LocalMqtt;
        }
    }

    static class FakeIotCoreMessageClient implements MessageClient<MqttMessage> {

        @Getter
        Set<String> subscriptions;

        @Override
        public void publish(MqttMessage message) throws MessageClientException {
        }

        @Override
        public void updateSubscriptions(Set<String> topics, Consumer<MqttMessage> messageHandler) {
            subscriptions = topics;
        }

        @Override
        public boolean supportsTopicFilters() {
            return true;
        }

        @Override
        public MqttMessage convertMessage(Message message) {
            return (MqttMessage) message.toMqtt();
        }

        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }

        @Override
        public TopicMapping.TopicType getType() {
            return TopicMapping.TopicType.IotCore;
        }
    }

    static class FakePubSubMessageClient implements MessageClient<PubSubMessage> {

        @Getter
        Set<String> subscriptions;
        
        @Override
        public void publish(PubSubMessage message) {
        }

        @Override
        public void updateSubscriptions(Set<String> topics, Consumer<PubSubMessage> messageHandler) {
            subscriptions = topics;
        }

        @Override
        public boolean supportsTopicFilters() {
            return true;
        }

        @Override
        public PubSubMessage convertMessage(Message message) {
            return (PubSubMessage) message.toPubSub();
        }

        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }

        @Override
        public TopicMapping.TopicType getType() {
            return TopicMapping.TopicType.Pubsub;
        }
    }
}
