/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge;

import com.aws.greengrass.mqttbridge.clients.MessageClient;
import com.aws.greengrass.mqttbridge.clients.MessageClientException;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.TestUtils;
import com.aws.greengrass.util.Utils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class MessageBridgeTest {
    @Mock
    private MessageClient mockMessageClient;
    @Mock
    private MessageClient mockMessageClient2;
    @Mock
    private MessageClient mockMessageClient3;

    @Mock
    private TopicMapping mockTopicMapping;

    private final ExecutorService mockExecutorService = TestUtils.synchronousExecutorService();

    @Test
    void WHEN_call_message_bridge_constructer_THEN_does_not_throw() {
        new MessageBridge(mockTopicMapping, mockExecutorService);
        verify(mockTopicMapping, times(1)).listenToUpdates(any());
    }

    @Test
    void GIVEN_mqtt_bridge_and_mapping_populated_WHEN_add_client_THEN_subscribed() throws Exception {
        TopicMapping mapping = new TopicMapping();
        Map<String, TopicMapping.MappingEntry> mappingToUpdate = Utils.immutableMap("m1",
                new TopicMapping.MappingEntry("mqtt/topic", TopicMapping.TopicType.LocalMqtt,
                        TopicMapping.TopicType.IotCore), "m2",
                new TopicMapping.MappingEntry("mqtt/topic2", TopicMapping.TopicType.LocalMqtt,
                        TopicMapping.TopicType.Pubsub), "m3",
                new TopicMapping.MappingEntry("mqtt/topic3", TopicMapping.TopicType.IotCore,
                        TopicMapping.TopicType.LocalMqtt), "m4",
                new TopicMapping.MappingEntry("mqtt/topic4", TopicMapping.TopicType.Pubsub,
                        TopicMapping.TopicType.LocalMqtt));
        mapping.updateMapping(mappingToUpdate);

        MessageBridge messageBridge = new MessageBridge(mapping, mockExecutorService);
        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.LocalMqtt, mockMessageClient);
        ArgumentCaptor<Set<String>> topicsArgumentCaptor = ArgumentCaptor.forClass(Set.class);
        verify(mockMessageClient, times(1)).updateSubscriptions(topicsArgumentCaptor.capture(), any());
        MatcherAssert.assertThat(topicsArgumentCaptor.getValue(), Matchers.hasSize(2));
        MatcherAssert
                .assertThat(topicsArgumentCaptor.getValue(), Matchers.containsInAnyOrder("mqtt/topic", "mqtt/topic2"));

        reset(mockMessageClient);
        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.Pubsub, mockMessageClient);
        topicsArgumentCaptor = ArgumentCaptor.forClass(Set.class);
        verify(mockMessageClient, times(1)).updateSubscriptions(topicsArgumentCaptor.capture(), any());
        MatcherAssert.assertThat(topicsArgumentCaptor.getValue(), Matchers.hasSize(1));
        MatcherAssert.assertThat(topicsArgumentCaptor.getValue(), Matchers.containsInAnyOrder("mqtt/topic4"));

        reset(mockMessageClient);
        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.IotCore, mockMessageClient);
        topicsArgumentCaptor = ArgumentCaptor.forClass(Set.class);
        verify(mockMessageClient, times(1)).updateSubscriptions(topicsArgumentCaptor.capture(), any());
        MatcherAssert.assertThat(topicsArgumentCaptor.getValue(), Matchers.hasSize(1));
        MatcherAssert.assertThat(topicsArgumentCaptor.getValue(), Matchers.containsInAnyOrder("mqtt/topic3"));
    }

    @Test
    void GIVEN_mqtt_bridge_and_clients_WHEN_mapping_populated_THEN_subscribed() throws Exception {
        TopicMapping mapping = new TopicMapping();
        MessageBridge messageBridge = new MessageBridge(mapping, mockExecutorService);

        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.LocalMqtt, mockMessageClient);
        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.Pubsub, mockMessageClient2);
        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.IotCore, mockMessageClient3);

        reset(mockMessageClient);
        reset(mockMessageClient2);
        reset(mockMessageClient3);

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
        mapping.updateMapping(mappingToUpdate);

        ArgumentCaptor<Set<String>> topicsArgumentCaptorLocalMqtt = ArgumentCaptor.forClass(Set.class);
        verify(mockMessageClient, times(1)).updateSubscriptions(topicsArgumentCaptorLocalMqtt.capture(), any());
        MatcherAssert.assertThat(topicsArgumentCaptorLocalMqtt.getValue(), Matchers.hasSize(3));
        MatcherAssert.assertThat(topicsArgumentCaptorLocalMqtt.getValue(),
                Matchers.containsInAnyOrder("mqtt/topic", "mqtt/topic2", "mqtt/topic/#"));

        ArgumentCaptor<Set<String>> topicsArgumentCaptorPubsub = ArgumentCaptor.forClass(Set.class);
        verify(mockMessageClient2, times(1)).updateSubscriptions(topicsArgumentCaptorPubsub.capture(), any());
        MatcherAssert.assertThat(topicsArgumentCaptorPubsub.getValue(), Matchers.hasSize(1));
        MatcherAssert.assertThat(topicsArgumentCaptorPubsub.getValue(), Matchers.containsInAnyOrder("mqtt/topic4"));

        ArgumentCaptor<Set<String>> topicsArgumentCaptorIotCore = ArgumentCaptor.forClass(Set.class);
        verify(mockMessageClient3, times(1)).updateSubscriptions(topicsArgumentCaptorIotCore.capture(), any());
        MatcherAssert.assertThat(topicsArgumentCaptorIotCore.getValue(), Matchers.hasSize(2));
        MatcherAssert.assertThat(topicsArgumentCaptorIotCore.getValue(),
                Matchers.containsInAnyOrder("mqtt/topic3", "mqtt/+/topic"));
    }

    @Test
    void GIVEN_mqtt_bridge_and_client_WHEN_client_removed_THEN_no_subscriptions_made() throws Exception {
        TopicMapping mapping = new TopicMapping();
        MessageBridge messageBridge = new MessageBridge(mapping, mockExecutorService);

        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.LocalMqtt, mockMessageClient);
        messageBridge.removeMessageClient(TopicMapping.TopicType.LocalMqtt);

        reset(mockMessageClient);
        Map<String, TopicMapping.MappingEntry> mappingToUpdate = Utils.immutableMap("m1",
                new TopicMapping.MappingEntry("mqtt/topic", TopicMapping.TopicType.LocalMqtt,
                        TopicMapping.TopicType.IotCore), "m2",
                new TopicMapping.MappingEntry("mqtt/topic2", TopicMapping.TopicType.LocalMqtt,
                        TopicMapping.TopicType.Pubsub), "m3",
                new TopicMapping.MappingEntry("mqtt/topic3", TopicMapping.TopicType.IotCore,
                        TopicMapping.TopicType.LocalMqtt), "m4",
                new TopicMapping.MappingEntry("mqtt/topic4", TopicMapping.TopicType.Pubsub,
                        TopicMapping.TopicType.LocalMqtt));
        mapping.updateMapping(mappingToUpdate);

        ArgumentCaptor<Set<String>> topicsArgumentCaptorLocalMqtt = ArgumentCaptor.forClass(Set.class);
        verify(mockMessageClient, times(0)).updateSubscriptions(topicsArgumentCaptorLocalMqtt.capture(), any());
    }

    @Test
    void GIVEN_mqtt_bridge_with_mapping_WHEN_mapping_updated_THEN_subscriptions_updated() throws Exception {
        TopicMapping mapping = new TopicMapping();
        MessageBridge messageBridge = new MessageBridge(mapping, mockExecutorService);

        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.LocalMqtt, mockMessageClient);
        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.Pubsub, mockMessageClient2);
        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.IotCore, mockMessageClient3);
        Map<String, TopicMapping.MappingEntry> mappingToUpdate = Utils.immutableMap("m1",
                new TopicMapping.MappingEntry("mqtt/topic", TopicMapping.TopicType.LocalMqtt,
                        TopicMapping.TopicType.IotCore), "m2",
                new TopicMapping.MappingEntry("mqtt/topic2", TopicMapping.TopicType.LocalMqtt,
                        TopicMapping.TopicType.Pubsub), "m3",
                new TopicMapping.MappingEntry("mqtt/topic3", TopicMapping.TopicType.IotCore,
                        TopicMapping.TopicType.LocalMqtt), "m4",
                new TopicMapping.MappingEntry("mqtt/topic4", TopicMapping.TopicType.Pubsub,
                        TopicMapping.TopicType.LocalMqtt));
        mapping.updateMapping(mappingToUpdate);

        reset(mockMessageClient);
        reset(mockMessageClient2);
        reset(mockMessageClient3);

        // Change topic 2
        // Add a new topic 3
        // Modify old topic 3 to come from Pubsub
        // Remove topic 4
        mappingToUpdate = Utils.immutableMap("m1",
                new TopicMapping.MappingEntry("mqtt/topic", TopicMapping.TopicType.LocalMqtt,
                        TopicMapping.TopicType.IotCore), "m2",
                new TopicMapping.MappingEntry("mqtt/topic2/changed", TopicMapping.TopicType.LocalMqtt,
                        TopicMapping.TopicType.Pubsub), "m3",
                new TopicMapping.MappingEntry("mqtt/topic3/added", TopicMapping.TopicType.LocalMqtt,
                        TopicMapping.TopicType.Pubsub), "m4",
                new TopicMapping.MappingEntry("mqtt/topic3", TopicMapping.TopicType.Pubsub,
                        TopicMapping.TopicType.LocalMqtt));
        mapping.updateMapping(mappingToUpdate);

        ArgumentCaptor<Set<String>> topicsArgumentCaptorLocalMqtt = ArgumentCaptor.forClass(Set.class);
        verify(mockMessageClient, times(1)).updateSubscriptions(topicsArgumentCaptorLocalMqtt.capture(), any());
        MatcherAssert.assertThat(topicsArgumentCaptorLocalMqtt.getValue(), Matchers.hasSize(3));
        MatcherAssert.assertThat(topicsArgumentCaptorLocalMqtt.getValue(),
                Matchers.containsInAnyOrder("mqtt/topic", "mqtt/topic2/changed", "mqtt/topic3/added"));

        ArgumentCaptor<Set<String>> topicsArgumentCaptorPubsub = ArgumentCaptor.forClass(Set.class);
        verify(mockMessageClient2, times(1)).updateSubscriptions(topicsArgumentCaptorPubsub.capture(), any());
        MatcherAssert.assertThat(topicsArgumentCaptorPubsub.getValue(), Matchers.hasSize(1));
        MatcherAssert.assertThat(topicsArgumentCaptorPubsub.getValue(), Matchers.containsInAnyOrder("mqtt/topic3"));

        ArgumentCaptor<Set<String>> topicsArgumentCaptorIotCore = ArgumentCaptor.forClass(Set.class);
        verify(mockMessageClient3, times(1)).updateSubscriptions(topicsArgumentCaptorIotCore.capture(), any());
        MatcherAssert.assertThat(topicsArgumentCaptorIotCore.getValue(), Matchers.hasSize(0));

        // Remove all
        reset(mockMessageClient);
        reset(mockMessageClient2);
        reset(mockMessageClient3);
        mapping.updateMapping(Collections.EMPTY_MAP);
        topicsArgumentCaptorLocalMqtt = ArgumentCaptor.forClass(Set.class);
        verify(mockMessageClient, times(1)).updateSubscriptions(topicsArgumentCaptorLocalMqtt.capture(), any());
        MatcherAssert.assertThat(topicsArgumentCaptorLocalMqtt.getValue(), Matchers.hasSize(0));

        topicsArgumentCaptorPubsub = ArgumentCaptor.forClass(Set.class);
        verify(mockMessageClient2, times(1)).updateSubscriptions(topicsArgumentCaptorPubsub.capture(), any());
        MatcherAssert.assertThat(topicsArgumentCaptorPubsub.getValue(), Matchers.hasSize(0));

        topicsArgumentCaptorIotCore = ArgumentCaptor.forClass(Set.class);
        verify(mockMessageClient3, times(1)).updateSubscriptions(topicsArgumentCaptorIotCore.capture(), any());
        MatcherAssert.assertThat(topicsArgumentCaptorIotCore.getValue(), Matchers.hasSize(0));
    }

    @Test
    void GIVEN_mqtt_bridge_and_mapping_populated_WHEN_receive_mqtt_message_THEN_routed_to_iotcore_pubsub()
            throws Exception {
        TopicMapping mapping = new TopicMapping();
        Map<String, TopicMapping.MappingEntry> mappingToUpdate = Utils.immutableMap("m1",
                new TopicMapping.MappingEntry("mqtt/topic", TopicMapping.TopicType.LocalMqtt,
                        TopicMapping.TopicType.IotCore), "m1-1",
                new TopicMapping.MappingEntry("mqtt/topic", TopicMapping.TopicType.LocalMqtt,
                        TopicMapping.TopicType.Pubsub), "m2",
                new TopicMapping.MappingEntry("mqtt/topic2", TopicMapping.TopicType.LocalMqtt,
                        TopicMapping.TopicType.Pubsub), "m3",
                new TopicMapping.MappingEntry("mqtt/topic3", TopicMapping.TopicType.IotCore,
                        TopicMapping.TopicType.LocalMqtt), "m4",
                new TopicMapping.MappingEntry("mqtt/topic4", TopicMapping.TopicType.Pubsub,
                        TopicMapping.TopicType.LocalMqtt));
        mapping.updateMapping(mappingToUpdate);

        MessageBridge messageBridge = new MessageBridge(mapping, mockExecutorService);

        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.LocalMqtt, mockMessageClient);
        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.Pubsub, mockMessageClient2);
        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.IotCore, mockMessageClient3);

        doReturn(true).when(mockMessageClient).supportsTopicFilters();

        ArgumentCaptor<Consumer> messageHandlerLocalMqttCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(mockMessageClient, times(1)).updateSubscriptions(any(), messageHandlerLocalMqttCaptor.capture());
        ArgumentCaptor<Consumer> messageHandlerPubSubCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(mockMessageClient2, times(1)).updateSubscriptions(any(), messageHandlerPubSubCaptor.capture());
        ArgumentCaptor<Consumer> messageHandlerIotCoreCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(mockMessageClient3, times(1)).updateSubscriptions(any(), messageHandlerIotCoreCaptor.capture());

        //Make mockMessageClient3 throw. (Will be ignored)
        doThrow(new MessageClientException("")).when(mockMessageClient3).publish(any());

        byte[] messageOnTopic1 = "message from topic mqtt/topic".getBytes();
        byte[] messageOnTopic2 = "message from topic mqtt/topic2".getBytes();
        messageHandlerLocalMqttCaptor.getValue().accept(new Message("mqtt/topic", messageOnTopic1));
        messageHandlerLocalMqttCaptor.getValue().accept(new Message("mqtt/topic2", messageOnTopic2));

        // Also send on an unknown topic
        messageHandlerLocalMqttCaptor.getValue().accept(new Message("mqtt/unknown", messageOnTopic2));

        verify(mockMessageClient, times(0)).publish(any());
        ArgumentCaptor<Message> messagePubSubCaptor = ArgumentCaptor.forClass(Message.class);
        verify(mockMessageClient2, times(2)).publish(messagePubSubCaptor.capture());
        ArgumentCaptor<Message> messageIotCoreCaptor = ArgumentCaptor.forClass(Message.class);
        verify(mockMessageClient3, times(1)).publish(messageIotCoreCaptor.capture());

        MatcherAssert.assertThat(messageIotCoreCaptor.getAllValues().get(0).getTopic(),
                Matchers.is(Matchers.equalTo("mqtt/topic")));
        Assertions.assertArrayEquals(messageOnTopic1, messageIotCoreCaptor.getAllValues().get(0).getPayload());

        MatcherAssert.assertThat(messagePubSubCaptor.getAllValues().get(0).getTopic(),
                Matchers.is(Matchers.equalTo("mqtt/topic")));
        Assertions.assertArrayEquals(messageOnTopic1, messagePubSubCaptor.getAllValues().get(0).getPayload());
        MatcherAssert.assertThat(messagePubSubCaptor.getAllValues().get(1).getTopic(),
                Matchers.is(Matchers.equalTo("mqtt/topic2")));
        Assertions.assertArrayEquals(messageOnTopic2, messagePubSubCaptor.getAllValues().get(1).getPayload());
    }

    @Test
    void GIVEN_mqtt_bridge_and_mapping_populated_with_filters_WHEN_receive_mqtt_message_THEN_routed_correctly()
            throws Exception {
        TopicMapping mapping = new TopicMapping();
        Map<String, TopicMapping.MappingEntry> mappingToUpdate = Utils.immutableMap("m1",
                new TopicMapping.MappingEntry("sensors/+/humidity", TopicMapping.TopicType.LocalMqtt,
                        TopicMapping.TopicType.IotCore), "m2",
                new TopicMapping.MappingEntry("sensors/satellite/#", TopicMapping.TopicType.IotCore,
                        TopicMapping.TopicType.Pubsub), "m3",
                new TopicMapping.MappingEntry("sensors/satellite/altitude", TopicMapping.TopicType.IotCore,
                        TopicMapping.TopicType.LocalMqtt), "m4",
                new TopicMapping.MappingEntry("sensors/thermostat1/humidity", TopicMapping.TopicType.LocalMqtt,
                        TopicMapping.TopicType.Pubsub), "m5",
                // This will cause a duplicate message to IoTCore
                // (one for sensors/+/humidity)
                new TopicMapping.MappingEntry("sensors/thermostat1/humidity", TopicMapping.TopicType.LocalMqtt,
                        TopicMapping.TopicType.IotCore));
        mapping.updateMapping(mappingToUpdate);

        MessageBridge messageBridge = new MessageBridge(mapping, mockExecutorService);

        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.LocalMqtt, mockMessageClient);
        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.Pubsub, mockMessageClient2);
        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.IotCore, mockMessageClient3);

        doReturn(true).when(mockMessageClient).supportsTopicFilters();
        doReturn(true).when(mockMessageClient3).supportsTopicFilters();

        ArgumentCaptor<Consumer> messageHandlerLocalMqttCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(mockMessageClient, times(1)).updateSubscriptions(any(), messageHandlerLocalMqttCaptor.capture());
        ArgumentCaptor<Consumer> messageHandlerPubSubCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(mockMessageClient2, times(1)).updateSubscriptions(any(), messageHandlerPubSubCaptor.capture());
        ArgumentCaptor<Consumer> messageHandlerIotCoreCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(mockMessageClient3, times(1)).updateSubscriptions(any(), messageHandlerIotCoreCaptor.capture());

        byte[] messageFromThermostat1 = "humidity = 40%".getBytes();
        byte[] messageFromThermostat2 = "humidity = 41%".getBytes();
        byte[] messageFromThermostat2Temp = "temperature = 70C".getBytes();
        messageHandlerLocalMqttCaptor.getValue()
                .accept(new Message("sensors/thermostat1/humidity", messageFromThermostat1));
        messageHandlerLocalMqttCaptor.getValue()
                .accept(new Message("sensors/thermostat2/humidity", messageFromThermostat2));

        // Also send for an unknown measurement
        messageHandlerLocalMqttCaptor.getValue()
                .accept(new Message("sensors/thermostat1/temperature", messageFromThermostat2Temp));
        // Also send for a topic with multiple nodes to match with the filter (which should not match)
        messageHandlerLocalMqttCaptor.getValue()
                .accept(new Message("sensors/thermostat2/zone1/humidity", messageFromThermostat2));

        verify(mockMessageClient, times(0)).publish(any());
        ArgumentCaptor<Message> messagePubSubCaptor = ArgumentCaptor.forClass(Message.class);
        verify(mockMessageClient2, times(1)).publish(messagePubSubCaptor.capture());
        ArgumentCaptor<Message> messageIotCoreCaptor = ArgumentCaptor.forClass(Message.class);
        verify(mockMessageClient3, times(3)).publish(messageIotCoreCaptor.capture());

        MatcherAssert.assertThat(messagePubSubCaptor.getAllValues().get(0).getTopic(),
                Matchers.is(Matchers.equalTo("sensors/thermostat1/humidity")));
        Assertions.assertArrayEquals(messageFromThermostat1, messagePubSubCaptor.getAllValues().get(0).getPayload());

        MatcherAssert.assertThat(messageIotCoreCaptor.getAllValues().get(0).getTopic(),
                Matchers.is(Matchers.equalTo("sensors/thermostat1/humidity")));
        Assertions.assertArrayEquals(messageFromThermostat1, messageIotCoreCaptor.getAllValues().get(0).getPayload());
        MatcherAssert.assertThat(messageIotCoreCaptor.getAllValues().get(1).getTopic(),
                Matchers.is(Matchers.equalTo("sensors/thermostat1/humidity")));
        Assertions.assertArrayEquals(messageFromThermostat1, messageIotCoreCaptor.getAllValues().get(1).getPayload());
        MatcherAssert.assertThat(messageIotCoreCaptor.getAllValues().get(2).getTopic(),
                Matchers.is(Matchers.equalTo("sensors/thermostat2/humidity")));
        Assertions.assertArrayEquals(messageFromThermostat2, messageIotCoreCaptor.getAllValues().get(2).getPayload());

        byte[] messageFromSatelliteForAltitude = "altitude = 10000".getBytes();
        byte[] messageFromSatelliteForConnectivity = "conn = 41%".getBytes();
        byte[] messageFromSatelliteForMultiLevel = "conn = 21%".getBytes();
        messageHandlerIotCoreCaptor.getValue()
                .accept(new Message("sensors/satellite/altitude", messageFromSatelliteForAltitude));
        messageHandlerIotCoreCaptor.getValue()
                .accept(new Message("sensors/satellite/connectivity", messageFromSatelliteForConnectivity));
        messageHandlerIotCoreCaptor.getValue()
                .accept(new Message("sensors/satellite/device1/connectivity", messageFromSatelliteForMultiLevel));

        ArgumentCaptor<Message> messageLocalMqttCaptor = ArgumentCaptor.forClass(Message.class);
        verify(mockMessageClient, times(1)).publish(messageLocalMqttCaptor.capture());
        messagePubSubCaptor = ArgumentCaptor.forClass(Message.class);
        verify(mockMessageClient2, times(1 + 3)).publish(messagePubSubCaptor.capture());
        messageIotCoreCaptor = ArgumentCaptor.forClass(Message.class);
        verify(mockMessageClient3, times(3)).publish(messageIotCoreCaptor.capture());

        MatcherAssert.assertThat(messageLocalMqttCaptor.getAllValues().get(0).getTopic(),
                Matchers.is(Matchers.equalTo("sensors/satellite/altitude")));
        Assertions.assertArrayEquals(messageFromSatelliteForAltitude,
                messageLocalMqttCaptor.getAllValues().get(0).getPayload());

        MatcherAssert.assertThat(messagePubSubCaptor.getAllValues().get(1).getTopic(),
                Matchers.is(Matchers.equalTo("sensors/satellite/altitude")));
        Assertions.assertArrayEquals(messageFromSatelliteForAltitude,
                messagePubSubCaptor.getAllValues().get(1).getPayload());
        MatcherAssert.assertThat(messagePubSubCaptor.getAllValues().get(2).getTopic(),
                Matchers.is(Matchers.equalTo("sensors/satellite/connectivity")));
        Assertions.assertArrayEquals(messageFromSatelliteForConnectivity,
                messagePubSubCaptor.getAllValues().get(2).getPayload());
        MatcherAssert.assertThat(messagePubSubCaptor.getAllValues().get(3).getTopic(),
                Matchers.is(Matchers.equalTo("sensors/satellite/device1/connectivity")));
        Assertions.assertArrayEquals(messageFromSatelliteForMultiLevel,
                messagePubSubCaptor.getAllValues().get(3).getPayload());
    }

    @Test
    void GIVEN_mqtt_bridge_and_mapping_populated_with_filters_in_pubsub_WHEN_receive_mqtt_message_THEN_routed_correctly()
            throws Exception {
        TopicMapping mapping = new TopicMapping();
        Map<String, TopicMapping.MappingEntry> mappingToUpdate = Utils.immutableMap("m1",
                new TopicMapping.MappingEntry("sensors/+/humidity", TopicMapping.TopicType.Pubsub,
                        TopicMapping.TopicType.IotCore), "m2",
                new TopicMapping.MappingEntry("sensors/thermostat1/humidity", TopicMapping.TopicType.Pubsub,
                        TopicMapping.TopicType.IotCore));
        mapping.updateMapping(mappingToUpdate);

        MessageBridge messageBridge = new MessageBridge(mapping, mockExecutorService);

        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.LocalMqtt, mockMessageClient);
        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.Pubsub, mockMessageClient2);
        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(TopicMapping.TopicType.IotCore, mockMessageClient3);

        ArgumentCaptor<Consumer> messageHandlerLocalMqttCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(mockMessageClient, times(1)).updateSubscriptions(any(), messageHandlerLocalMqttCaptor.capture());
        ArgumentCaptor<Consumer> messageHandlerPubSubCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(mockMessageClient2, times(1)).updateSubscriptions(any(), messageHandlerPubSubCaptor.capture());
        ArgumentCaptor<Consumer> messageHandlerIotCoreCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(mockMessageClient3, times(1)).updateSubscriptions(any(), messageHandlerIotCoreCaptor.capture());

        byte[] messageFromThermostat1 = "humidity = 40%".getBytes();
        messageHandlerPubSubCaptor.getValue()
                .accept(new Message("sensors/thermostat1/humidity", messageFromThermostat1));

        verify(mockMessageClient, times(0)).publish(any());
        verify(mockMessageClient2, times(0)).publish(any());
        ArgumentCaptor<Message> messageIotCoreCaptor = ArgumentCaptor.forClass(Message.class);
        verify(mockMessageClient3, times(1)).publish(messageIotCoreCaptor.capture());

        MatcherAssert.assertThat(messageIotCoreCaptor.getAllValues().get(0).getTopic(),
                Matchers.is(Matchers.equalTo("sensors/thermostat1/humidity")));
        Assertions.assertArrayEquals(messageFromThermostat1, messageIotCoreCaptor.getAllValues().get(0).getPayload());
    }
}
