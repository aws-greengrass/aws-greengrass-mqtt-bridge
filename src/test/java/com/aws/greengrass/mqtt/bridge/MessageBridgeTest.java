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
import lombok.Getter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;


import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith({MockitoExtension.class, GGExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
public class MessageBridgeTest {

    @Spy
    FakeLocalMessageClient mockLocalClient;
    @Spy
    FakePubSubMessageClient mockPubSubClient;
    @Spy
    FakeIotCoreMessageClient mockIotCoreClient;
    @Mock
    BridgeConfig bridgeConfig;
    TopicMapping topicMapping = new TopicMapping();
    MessageBridge messageBridge;

    @BeforeEach
    void setUp() throws MessageClientException {
        BridgeConfigReference bridgeConfig = new BridgeConfigReference();
        bridgeConfig.set(this.bridgeConfig);
        messageBridge = new MessageBridge(topicMapping, bridgeConfig, new MessageClients(mockIotCoreClient, mockLocalClient, mockPubSubClient));
        messageBridge.initialize();
    }

    @Test
    void GIVEN_mqtt_bridge_and_clients_WHEN_mapping_populated_THEN_subscribed() {
        Map<String, TopicMapping.MappingEntry> mappingToUpdate = Utils.immutableMap("m1",
                new TopicMapping.MappingEntry("mqtt/topic", TopicMapping.TopicType.LocalMqtt,
                        TopicMapping.TopicType.IotCore), "m2",
                new TopicMapping.MappingEntry("mqtt/topic2", TopicMapping.TopicType.LocalMqtt,
                        TopicMapping.TopicType.Pubsub), "m3",
                new TopicMapping.MappingEntry("mqtt/topic3", TopicMapping.TopicType.IotCore,
                        TopicMapping.TopicType.LocalMqtt), "m4",
                new TopicMapping.MappingEntry("mqtt/topic4", TopicMapping.TopicType.Pubsub,
                        TopicMapping.TopicType.LocalMqtt), "m5",
                new TopicMapping.MappingEntry("mqtt/+/topic", TopicMapping.TopicType.IotCore,
                        TopicMapping.TopicType.LocalMqtt), "m6",
                new TopicMapping.MappingEntry("mqtt/topic/#", TopicMapping.TopicType.LocalMqtt,
                        TopicMapping.TopicType.IotCore));
        topicMapping.updateMapping(mappingToUpdate);

        Set<String> localTopics = new HashSet<>(Arrays.asList("mqtt/topic", "mqtt/topic2", "mqtt/topic/#"));
        assertEquals(localTopics, mockLocalClient.getSubscriptions());

        Set<String> pubSubTopics = Collections.singleton("mqtt/topic4");
        assertEquals(pubSubTopics, mockPubSubClient.getSubscriptions());

        Set<String> iotCoreTopics = new HashSet<>(Arrays.asList("mqtt/topic3", "mqtt/+/topic"));
        assertEquals(iotCoreTopics, mockIotCoreClient.getSubscriptions());
    }

    // TODO

//    @Test
//    @SuppressWarnings("unchecked")
//    void GIVEN_mqtt_bridge_and_client_WHEN_client_removed_THEN_no_subscriptions_made() {
//        TopicMapping mapping = new TopicMapping();
//        MessageBridge messageBridge = new MessageBridge(mapping, Collections.emptyMap());
//
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.LocalMqtt, mockLocalClient);
//        messageBridge.removeMessageClient(TopicMapping.TopicType.LocalMqtt);
//
//        reset(mockLocalClient);
//        Map<String, TopicMapping.MappingEntry> mappingToUpdate = Utils.immutableMap("m1",
//                new TopicMapping.MappingEntry("mqtt/topic", TopicMapping.TopicType.LocalMqtt,
//                        TopicMapping.TopicType.IotCore), "m2",
//                new TopicMapping.MappingEntry("mqtt/topic2", TopicMapping.TopicType.LocalMqtt,
//                        TopicMapping.TopicType.Pubsub), "m3",
//                new TopicMapping.MappingEntry("mqtt/topic3", TopicMapping.TopicType.IotCore,
//                        TopicMapping.TopicType.LocalMqtt), "m4",
//                new TopicMapping.MappingEntry("mqtt/topic4", TopicMapping.TopicType.Pubsub,
//                        TopicMapping.TopicType.LocalMqtt));
//        mapping.updateMapping(mappingToUpdate);
//
//        ArgumentCaptor<Set<String>> topicsArgumentCaptorLocalMqtt = ArgumentCaptor.forClass(Set.class);
//        verify(mockLocalClient, times(0)).updateSubscriptions(topicsArgumentCaptorLocalMqtt.capture(), any());
//    }
//
//    @Test
//    @SuppressWarnings("unchecked")
//    void GIVEN_mqtt_bridge_with_mapping_WHEN_mapping_updated_THEN_subscriptions_updated() {
//        TopicMapping mapping = new TopicMapping();
//        MessageBridge messageBridge = new MessageBridge(mapping, Collections.emptyMap());
//
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.LocalMqtt, mockLocalClient);
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.Pubsub, mockPubSubClient);
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.IotCore, mockIotCoreClient);
//        Map<String, TopicMapping.MappingEntry> mappingToUpdate = Utils.immutableMap("m1",
//                new TopicMapping.MappingEntry("mqtt/topic", TopicMapping.TopicType.LocalMqtt,
//                        TopicMapping.TopicType.IotCore), "m2",
//                new TopicMapping.MappingEntry("mqtt/topic2", TopicMapping.TopicType.LocalMqtt,
//                        TopicMapping.TopicType.Pubsub), "m3",
//                new TopicMapping.MappingEntry("mqtt/topic3", TopicMapping.TopicType.IotCore,
//                        TopicMapping.TopicType.LocalMqtt), "m4",
//                new TopicMapping.MappingEntry("mqtt/topic4", TopicMapping.TopicType.Pubsub,
//                        TopicMapping.TopicType.LocalMqtt));
//        mapping.updateMapping(mappingToUpdate);
//
//        reset(mockLocalClient);
//        reset(mockPubSubClient);
//        reset(mockIotCoreClient);
//
//        // Change topic 2
//        // Add a new topic 3
//        // Modify old topic 3 to come from Pubsub
//        // Remove topic 4
//        mappingToUpdate = Utils.immutableMap("m1",
//                new TopicMapping.MappingEntry("mqtt/topic", TopicMapping.TopicType.LocalMqtt,
//                        TopicMapping.TopicType.IotCore), "m2",
//                new TopicMapping.MappingEntry("mqtt/topic2/changed", TopicMapping.TopicType.LocalMqtt,
//                        TopicMapping.TopicType.Pubsub), "m3",
//                new TopicMapping.MappingEntry("mqtt/topic3/added", TopicMapping.TopicType.LocalMqtt,
//                        TopicMapping.TopicType.Pubsub), "m4",
//                new TopicMapping.MappingEntry("mqtt/topic3", TopicMapping.TopicType.Pubsub,
//                        TopicMapping.TopicType.LocalMqtt));
//        mapping.updateMapping(mappingToUpdate);
//
//        ArgumentCaptor<Set<String>> topicsArgumentCaptorLocalMqtt = ArgumentCaptor.forClass(Set.class);
//        verify(mockLocalClient, times(1)).updateSubscriptions(topicsArgumentCaptorLocalMqtt.capture(), any());
//        assertThat(topicsArgumentCaptorLocalMqtt.getValue(), Matchers.hasSize(3));
//        assertThat(topicsArgumentCaptorLocalMqtt.getValue(),
//                Matchers.containsInAnyOrder("mqtt/topic", "mqtt/topic2/changed", "mqtt/topic3/added"));
//
//        ArgumentCaptor<Set<String>> topicsArgumentCaptorPubsub = ArgumentCaptor.forClass(Set.class);
//        verify(mockPubSubClient, times(1)).updateSubscriptions(topicsArgumentCaptorPubsub.capture(), any());
//        assertThat(topicsArgumentCaptorPubsub.getValue(), Matchers.hasSize(1));
//        assertThat(topicsArgumentCaptorPubsub.getValue(), Matchers.containsInAnyOrder("mqtt/topic3"));
//
//        ArgumentCaptor<Set<String>> topicsArgumentCaptorIotCore = ArgumentCaptor.forClass(Set.class);
//        verify(mockIotCoreClient, times(1)).updateSubscriptions(topicsArgumentCaptorIotCore.capture(), any());
//        assertThat(topicsArgumentCaptorIotCore.getValue(), Matchers.hasSize(0));
//
//        // Remove all
//        reset(mockLocalClient);
//        reset(mockPubSubClient);
//        reset(mockIotCoreClient);
//        mapping.updateMapping(Collections.emptyMap());
//        topicsArgumentCaptorLocalMqtt = ArgumentCaptor.forClass(Set.class);
//        verify(mockLocalClient, times(1)).updateSubscriptions(topicsArgumentCaptorLocalMqtt.capture(), any());
//        assertThat(topicsArgumentCaptorLocalMqtt.getValue(), Matchers.hasSize(0));
//
//        topicsArgumentCaptorPubsub = ArgumentCaptor.forClass(Set.class);
//        verify(mockPubSubClient, times(1)).updateSubscriptions(topicsArgumentCaptorPubsub.capture(), any());
//        assertThat(topicsArgumentCaptorPubsub.getValue(), Matchers.hasSize(0));
//
//        topicsArgumentCaptorIotCore = ArgumentCaptor.forClass(Set.class);
//        verify(mockIotCoreClient, times(1)).updateSubscriptions(topicsArgumentCaptorIotCore.capture(), any());
//        assertThat(topicsArgumentCaptorIotCore.getValue(), Matchers.hasSize(0));
//    }
//
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
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.LocalMqtt, mockLocalClient);
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.Pubsub, mockPubSubClient);
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.IotCore, mockIotCoreClient);
//
//        ArgumentCaptor<Consumer<MqttMessage>> messageHandlerLocalMqttCaptor = ArgumentCaptor.forClass(Consumer.class);
//        verify(mockLocalClient, times(1)).updateSubscriptions(any(), messageHandlerLocalMqttCaptor.capture());
//        ArgumentCaptor<Consumer<PubSubMessage>> messageHandlerPubSubCaptor = ArgumentCaptor.forClass(Consumer.class);
//        verify(mockPubSubClient, times(1)).updateSubscriptions(any(), messageHandlerPubSubCaptor.capture());
//        ArgumentCaptor<Consumer<MqttMessage>> messageHandlerIotCoreCaptor = ArgumentCaptor.forClass(Consumer.class);
//        verify(mockIotCoreClient, times(1)).updateSubscriptions(any(), messageHandlerIotCoreCaptor.capture());
//
//        //Make mockMessageClient3 throw. (Will be ignored)
//        doThrow(new MessageClientException("")).when(mockIotCoreClient).publish(any());
//        byte[] messageOnTopic1 = "message from topic mqtt/topic".getBytes();
//        byte[] messageOnTopic2 = "message from topic mqtt/topic2".getBytes();
//        messageHandlerLocalMqttCaptor.getValue().accept(MqttMessage.builder().topic("mqtt/topic").payload(messageOnTopic1).build());
//        messageHandlerLocalMqttCaptor.getValue().accept(MqttMessage.builder().topic("mqtt/topic2").payload(messageOnTopic2).build());
//
//        // Also send on an unknown topic
//        messageHandlerLocalMqttCaptor.getValue().accept(MqttMessage.builder().topic("mqtt/unknown").payload(messageOnTopic2).build());
//
//        verify(mockLocalClient, times(0)).publish(any());
//        ArgumentCaptor<PubSubMessage> messagePubSubCaptor = ArgumentCaptor.forClass(PubSubMessage.class);
//        verify(mockPubSubClient, times(2)).publish(messagePubSubCaptor.capture());
//        ArgumentCaptor<MqttMessage> messageIotCoreCaptor = ArgumentCaptor.forClass(MqttMessage.class);
//        verify(mockIotCoreClient, times(1)).publish(messageIotCoreCaptor.capture());
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
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.LocalMqtt, mockLocalClient);
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.Pubsub, mockPubSubClient);
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.IotCore, mockIotCoreClient);
//
//        ArgumentCaptor<Consumer<MqttMessage>> messageHandlerLocalMqttCaptor = ArgumentCaptor.forClass(Consumer.class);
//        verify(mockLocalClient, times(1)).updateSubscriptions(any(), messageHandlerLocalMqttCaptor.capture());
//        ArgumentCaptor<Consumer<PubSubMessage>> messageHandlerPubSubCaptor = ArgumentCaptor.forClass(Consumer.class);
//        verify(mockPubSubClient, times(1)).updateSubscriptions(any(), messageHandlerPubSubCaptor.capture());
//        ArgumentCaptor<Consumer<MqttMessage>> messageHandlerIotCoreCaptor = ArgumentCaptor.forClass(Consumer.class);
//        verify(mockIotCoreClient, times(1)).updateSubscriptions(any(), messageHandlerIotCoreCaptor.capture());
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
//        verify(mockLocalClient, times(0)).publish(any());
//        ArgumentCaptor<PubSubMessage> messagePubSubCaptor = ArgumentCaptor.forClass(PubSubMessage.class);
//        verify(mockPubSubClient, times(1)).publish(messagePubSubCaptor.capture());
//        ArgumentCaptor<MqttMessage> messageIotCoreCaptor = ArgumentCaptor.forClass(MqttMessage.class);
//        verify(mockIotCoreClient, times(3)).publish(messageIotCoreCaptor.capture());
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
//        verify(mockLocalClient, times(1)).publish(messageLocalMqttCaptor.capture());
//        messagePubSubCaptor = ArgumentCaptor.forClass(PubSubMessage.class);
//        verify(mockPubSubClient, times(1 + 3)).publish(messagePubSubCaptor.capture());
//        messageIotCoreCaptor = ArgumentCaptor.forClass(MqttMessage.class);
//        verify(mockIotCoreClient, times(3)).publish(messageIotCoreCaptor.capture());
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
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.LocalMqtt, mockLocalClient);
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.Pubsub, mockPubSubClient);
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.IotCore, mockIotCoreClient);
//
//        ArgumentCaptor<Consumer<MqttMessage>> messageHandlerLocalMqttCaptor = ArgumentCaptor.forClass(Consumer.class);
//        verify(mockLocalClient, times(1)).updateSubscriptions(any(), messageHandlerLocalMqttCaptor.capture());
//        ArgumentCaptor<Consumer<PubSubMessage>> messageHandlerPubSubCaptor = ArgumentCaptor.forClass(Consumer.class);
//        verify(mockPubSubClient, times(1)).updateSubscriptions(any(), messageHandlerPubSubCaptor.capture());
//        ArgumentCaptor<Consumer<MqttMessage>> messageHandlerIotCoreCaptor = ArgumentCaptor.forClass(Consumer.class);
//        verify(mockIotCoreClient, times(1)).updateSubscriptions(any(), messageHandlerIotCoreCaptor.capture());
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
//        verify(mockLocalClient, times(0)).publish(any());
//        ArgumentCaptor<PubSubMessage> messagePubSubCaptor = ArgumentCaptor.forClass(PubSubMessage.class);
//        verify(mockPubSubClient, times(1)).publish(messagePubSubCaptor.capture());
//        ArgumentCaptor<MqttMessage> messageIotCoreCaptor = ArgumentCaptor.forClass(MqttMessage.class);
//        verify(mockIotCoreClient, times(3)).publish(messageIotCoreCaptor.capture());
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
//        verify(mockLocalClient, times(1)).publish(messageLocalMqttCaptor.capture());
//        messagePubSubCaptor = ArgumentCaptor.forClass(PubSubMessage.class);
//        verify(mockPubSubClient, times(1 + 3)).publish(messagePubSubCaptor.capture());
//        messageIotCoreCaptor = ArgumentCaptor.forClass(MqttMessage.class);
//        verify(mockIotCoreClient, times(3)).publish(messageIotCoreCaptor.capture());
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
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.LocalMqtt, mockLocalClient);
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.Pubsub, mockPubSubClient);
//        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.IotCore, mockIotCoreClient);
//
//        ArgumentCaptor<Consumer<MqttMessage>> messageHandlerLocalMqttCaptor = ArgumentCaptor.forClass(Consumer.class);
//        verify(mockLocalClient, times(1)).updateSubscriptions(any(), messageHandlerLocalMqttCaptor.capture());
//        ArgumentCaptor<Consumer<PubSubMessage>> messageHandlerPubSubCaptor = ArgumentCaptor.forClass(Consumer.class);
//        verify(mockPubSubClient, times(1)).updateSubscriptions(any(), messageHandlerPubSubCaptor.capture());
//        ArgumentCaptor<Consumer<MqttMessage>> messageHandlerIotCoreCaptor = ArgumentCaptor.forClass(Consumer.class);
//        verify(mockIotCoreClient, times(1)).updateSubscriptions(any(), messageHandlerIotCoreCaptor.capture());
//
//        byte[] messageFromThermostat1 = "humidity = 40%".getBytes();
//        messageHandlerPubSubCaptor.getValue()
//                .accept(new PubSubMessage("sensors/thermostat1/humidity", messageFromThermostat1));
//
//        verify(mockLocalClient, times(0)).publish(any());
//        verify(mockPubSubClient, times(0)).publish(any());
//        ArgumentCaptor<MqttMessage> messageIotCoreCaptor = ArgumentCaptor.forClass(MqttMessage.class);
//        verify(mockIotCoreClient, times(2)).publish(messageIotCoreCaptor.capture());
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
//                mockLocalClient);
//        ArgumentCaptor<Consumer<MqttMessage>> messageHandlerLocalMqttCaptor = ArgumentCaptor.forClass(Consumer.class);
//        verify(mockLocalClient, times(1)).updateSubscriptions(any(), messageHandlerLocalMqttCaptor.capture());
//        messageHandlerLocalMqttCaptor.getValue()
//                .accept(MqttMessage.builder().topic("topics/toLocal").payload(payload).retain(true).build());
//
//        ArgumentCaptor<MqttMessage> messageLocalMqttCaptor = ArgumentCaptor.forClass(MqttMessage.class);
//        verify(mockLocalClient, times(1)).publish(messageLocalMqttCaptor.capture());
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
//                mockLocalClient);
//        ArgumentCaptor<Consumer<MqttMessage>> messageHandlerLocalMqttCaptor = ArgumentCaptor.forClass(Consumer.class);
//        verify(mockLocalClient, times(1)).updateSubscriptions(any(), messageHandlerLocalMqttCaptor.capture());
//        messageHandlerLocalMqttCaptor.getValue()
//                .accept(MqttMessage.builder().topic("topics/toLocal").payload(payload).retain(true).build());
//
//        ArgumentCaptor<MqttMessage> messageLocalMqttCaptor = ArgumentCaptor.forClass(MqttMessage.class);
//        verify(mockLocalClient, times(1)).publish(messageLocalMqttCaptor.capture());
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
