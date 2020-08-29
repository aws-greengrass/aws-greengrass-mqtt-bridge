/* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0 */

package com.aws.iot.evergreen.mqtt.bridge.clients;

import com.aws.iot.evergreen.config.Topics;
import com.aws.iot.evergreen.mqtt.bridge.Message;
import com.aws.iot.evergreen.mqtt.bridge.MessageBridge;
import com.aws.iot.evergreen.mqtt.bridge.TopicMapping;
import com.aws.iot.evergreen.testcommons.testutilities.EGExtension;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;


@ExtendWith({MockitoExtension.class, EGExtension.class})
public class MQTTClientTest {

    private static final String SERVER_URI = "testUri";
    private static final String CLIENT_ID = "id";

    @Mock
    private Topics mockTopics;

    @Mock
    private MqttClient mockMqttClient;

    @Mock
    private TopicMapping mockTopicMapping;

    @Mock
    private MessageBridge mockMessageBridge;

    @Test
    void WHEN_call_mqtt_client_constructed_THEN_does_not_throw() {
        TopicMapping mapping = new TopicMapping();
        when(mockTopics.findOrDefault(any(), eq(MQTTClient.BROKER_URI_KEY))).thenReturn(SERVER_URI);
        when(mockTopics.findOrDefault(any(), eq(MQTTClient.CLIENT_ID_KEY))).thenReturn(CLIENT_ID);
        new MQTTClient(mockTopics, mapping, new MessageBridge(), mockMqttClient);
    }

    @Test
    void GIVEN_mqtt_client_WHEN_call_start_THEN_connected_to_broker() throws Exception {
        when(mockTopics.findOrDefault(any(), eq(MQTTClient.BROKER_URI_KEY))).thenReturn(SERVER_URI);
        when(mockTopics.findOrDefault(any(), eq(MQTTClient.CLIENT_ID_KEY))).thenReturn(CLIENT_ID);
        doNothing().when(mockMqttClient).connect(any(MqttConnectOptions.class));
        doNothing().when(mockMqttClient).setCallback(any());
        MQTTClient mqttClient = new MQTTClient(mockTopics, mockTopicMapping, mockMessageBridge, mockMqttClient);
        mqttClient.start();
        verify(mockMqttClient, times(1)).connect(any());
        verify(mockMqttClient, times(1)).setCallback(any());
        verify(mockTopicMapping, times(1)).listenToUpdates(any());
        ArgumentCaptor<TopicMapping.TopicType> sourceTypeArgumentCaptor =
                ArgumentCaptor.forClass(TopicMapping.TopicType.class);
        verify(mockMessageBridge, times(2)).addListener(any(), sourceTypeArgumentCaptor.capture());
        MatcherAssert.assertThat(sourceTypeArgumentCaptor.getAllValues(),
                Matchers.containsInAnyOrder(TopicMapping.TopicType.IotCore, TopicMapping.TopicType.Pubsub));
    }

    @Test
    void GIVEN_mqtt_client_WHEN_start_throws_exception_THEN_mqtt_client_exception_is_thrown() throws Exception {
        when(mockTopics.findOrDefault(any(), eq(MQTTClient.BROKER_URI_KEY))).thenReturn(SERVER_URI);
        when(mockTopics.findOrDefault(any(), eq(MQTTClient.CLIENT_ID_KEY))).thenReturn(CLIENT_ID);
        doThrow(new MqttException(0)).when(mockMqttClient).connect(any(MqttConnectOptions.class));
        MQTTClient mqttClient = new MQTTClient(mockTopics, mockTopicMapping, mockMessageBridge, mockMqttClient);
        Assertions.assertThrows(MQTTClientException.class, mqttClient::start);
    }

    @Test
    void GIVEN_mqtt_client_and_mapping_populated_WHEN_call_start_THEN_internal_mappings_updated_and_subscribed()
            throws Exception {
        TopicMapping mapping = new TopicMapping();
        mapping.updateMapping("[\n"
                + "  {\"SourceTopic\": \"mqtt/topic\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/cloud/topic\", \"DestTopicType\": \"IotCore\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic2\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/pubsub/topic\", \"DestTopicType\": \"Pubsub\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic3\", \"SourceTopicType\": \"IotCore\", \"DestTopic\": "
                + "\"/test/cloud/topic2\", \"DestTopicType\": \"LocalMqtt\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic4\", \"SourceTopicType\": \"Pubsub\", \"DestTopic\": "
                + "\"/test/cloud/topic2\", \"DestTopicType\": \"LocalMqtt\"}\n" + "]");
        when(mockTopics.findOrDefault(any(), eq(MQTTClient.BROKER_URI_KEY))).thenReturn(SERVER_URI);
        when(mockTopics.findOrDefault(any(), eq(MQTTClient.CLIENT_ID_KEY))).thenReturn(CLIENT_ID);
        doNothing().when(mockMqttClient).connect(any(MqttConnectOptions.class));
        doNothing().when(mockMqttClient).setCallback(any());
        MQTTClient mqttClient = new MQTTClient(mockTopics, mapping, mockMessageBridge, mockMqttClient);
        mqttClient.start();
        ArgumentCaptor<String> topicArgumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockMqttClient, times(2)).subscribe(topicArgumentCaptor.capture());
        MatcherAssert.assertThat(topicArgumentCaptor.getAllValues(),
                Matchers.containsInAnyOrder("mqtt/topic", "mqtt/topic2"));

        List<TopicMapping.MappingEntry> subscribedLocalMqttTopics = mqttClient.getSubscribedLocalMqttTopics();
        MatcherAssert.assertThat(subscribedLocalMqttTopics, Matchers.hasSize(2));
        MatcherAssert.assertThat(subscribedLocalMqttTopics, Matchers.containsInAnyOrder(
                new TopicMapping.MappingEntry("mqtt/topic", TopicMapping.TopicType.LocalMqtt, "/test/cloud/topic",
                        TopicMapping.TopicType.IotCore),
                new TopicMapping.MappingEntry("mqtt/topic2", TopicMapping.TopicType.LocalMqtt, "/test/pubsub/topic",
                        TopicMapping.TopicType.Pubsub)));

        List<TopicMapping.MappingEntry> topicMappingsWithDestinationAsLocalMqtt =
                mqttClient.getTopicMappingsWithDestinationAsLocalMqtt();
        MatcherAssert.assertThat(topicMappingsWithDestinationAsLocalMqtt, Matchers.hasSize(2));
        MatcherAssert.assertThat(topicMappingsWithDestinationAsLocalMqtt, Matchers.containsInAnyOrder(
                new TopicMapping.MappingEntry("mqtt/topic3", TopicMapping.TopicType.IotCore, "/test/cloud/topic2",
                        TopicMapping.TopicType.LocalMqtt),
                new TopicMapping.MappingEntry("mqtt/topic4", TopicMapping.TopicType.Pubsub, "/test/cloud/topic2",
                        TopicMapping.TopicType.LocalMqtt)));
    }

    @Test
    void GIVEN_mqtt_client_with_mapping_WHEN_call_stop_THEN_internal_mappings_deleted_and_unsubscribed()
            throws Exception {
        TopicMapping mapping = new TopicMapping();
        mapping.updateMapping("[\n"
                + "  {\"SourceTopic\": \"mqtt/topic\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/cloud/topic\", \"DestTopicType\": \"IotCore\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic2\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/pubsub/topic\", \"DestTopicType\": \"Pubsub\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic3\", \"SourceTopicType\": \"IotCore\", \"DestTopic\": "
                + "\"/test/cloud/topic2\", \"DestTopicType\": \"LocalMqtt\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic4\", \"SourceTopicType\": \"Pubsub\", \"DestTopic\": "
                + "\"/test/cloud/topic2\", \"DestTopicType\": \"LocalMqtt\"}\n" + "]");
        when(mockTopics.findOrDefault(any(), eq(MQTTClient.BROKER_URI_KEY))).thenReturn(SERVER_URI);
        when(mockTopics.findOrDefault(any(), eq(MQTTClient.CLIENT_ID_KEY))).thenReturn(CLIENT_ID);
        doNothing().when(mockMqttClient).connect(any(MqttConnectOptions.class));
        doNothing().when(mockMqttClient).setCallback(any());
        MQTTClient mqttClient = new MQTTClient(mockTopics, mapping, mockMessageBridge, mockMqttClient);
        mqttClient.start();

        mqttClient.stop();

        ArgumentCaptor<String> topicArgumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockMqttClient, times(2)).unsubscribe(topicArgumentCaptor.capture());
        MatcherAssert.assertThat(topicArgumentCaptor.getAllValues(),
                Matchers.containsInAnyOrder("mqtt/topic", "mqtt/topic2"));

        List<TopicMapping.MappingEntry> subscribedLocalMqttTopics = mqttClient.getSubscribedLocalMqttTopics();
        MatcherAssert.assertThat(subscribedLocalMqttTopics, Matchers.hasSize(0));

        List<TopicMapping.MappingEntry> topicMappingsWithDestinationAsLocalMqtt =
                mqttClient.getTopicMappingsWithDestinationAsLocalMqtt();
        MatcherAssert.assertThat(topicMappingsWithDestinationAsLocalMqtt, Matchers.hasSize(0));

        verify(mockMqttClient, times(1)).disconnect();
        ArgumentCaptor<TopicMapping.TopicType> sourceTypeArgumentCaptor =
                ArgumentCaptor.forClass(TopicMapping.TopicType.class);
        verify(mockMessageBridge, times(2)).removeListener(any(), sourceTypeArgumentCaptor.capture());
        MatcherAssert.assertThat(sourceTypeArgumentCaptor.getAllValues(),
                Matchers.containsInAnyOrder(TopicMapping.TopicType.IotCore, TopicMapping.TopicType.Pubsub));
    }

    @Test
    void GIVEN_mqtt_client_with_mapping_WHEN_mapping_updated_THEN_internal_mappings_and_subscriptions_updated()
            throws Exception {
        TopicMapping mapping = new TopicMapping();
        mapping.updateMapping("[\n"
                + "  {\"SourceTopic\": \"mqtt/topic\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/cloud/topic\", \"DestTopicType\": \"IotCore\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic2\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/pubsub/topic\", \"DestTopicType\": \"Pubsub\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic3\", \"SourceTopicType\": \"IotCore\", \"DestTopic\": "
                + "\"/test/cloud/topic2\", \"DestTopicType\": \"LocalMqtt\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic4\", \"SourceTopicType\": \"Pubsub\", \"DestTopic\": "
                + "\"/test/cloud/topic2\", \"DestTopicType\": \"LocalMqtt\"}\n" + "]");
        when(mockTopics.findOrDefault(any(), eq(MQTTClient.BROKER_URI_KEY))).thenReturn(SERVER_URI);
        when(mockTopics.findOrDefault(any(), eq(MQTTClient.CLIENT_ID_KEY))).thenReturn(CLIENT_ID);
        doNothing().when(mockMqttClient).connect(any(MqttConnectOptions.class));
        doNothing().when(mockMqttClient).setCallback(any());
        MQTTClient mqttClient = new MQTTClient(mockTopics, mapping, mockMessageBridge, mockMqttClient);
        mqttClient.start();

        reset(mockMqttClient);
        // Change topic 2
        // Add a new topic 3
        // Modify old topic 3 to come from Pubsub
        // Remove topic 4
        mapping.updateMapping(
                "[\n" + "  {\"SourceTopic\": \"mqtt/topic\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": "
                        + "\"/test/cloud/topic\", \"DestTopicType\": \"IotCore\"},\n"
                        + "  {\"SourceTopic\": \"mqtt/topic2/changed\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": "
                        + "\"/test/pubsub/topic/changed\", \"DestTopicType\": \"Pubsub\"},\n"
                        + "  {\"SourceTopic\": \"mqtt/topic3/added\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": "
                        + "\"/test/pubsub/topic/added\", \"DestTopicType\": \"Pubsub\"},\n"
                        + "  {\"SourceTopic\": \"mqtt/topic3\", \"SourceTopicType\": \"Pubsub\", \"DestTopic\": "
                        + "\"/test/pubsub/topic2\", \"DestTopicType\": \"LocalMqtt\"}\n" + "]");

        ArgumentCaptor<String> topicArgumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockMqttClient, times(2)).subscribe(topicArgumentCaptor.capture());
        MatcherAssert.assertThat(topicArgumentCaptor.getAllValues(),
                Matchers.containsInAnyOrder("mqtt/topic2/changed", "mqtt/topic3/added"));

        List<TopicMapping.MappingEntry> subscribedLocalMqttTopics = mqttClient.getSubscribedLocalMqttTopics();
        MatcherAssert.assertThat(subscribedLocalMqttTopics, Matchers.hasSize(3));
        MatcherAssert.assertThat(subscribedLocalMqttTopics, Matchers.containsInAnyOrder(
                new TopicMapping.MappingEntry("mqtt/topic", TopicMapping.TopicType.LocalMqtt, "/test/cloud/topic",
                        TopicMapping.TopicType.IotCore),
                new TopicMapping.MappingEntry("mqtt/topic2/changed", TopicMapping.TopicType.LocalMqtt,
                        "/test/pubsub/topic/changed", TopicMapping.TopicType.Pubsub),
                new TopicMapping.MappingEntry("mqtt/topic3/added", TopicMapping.TopicType.LocalMqtt,
                        "/test/pubsub" + "/topic/added", TopicMapping.TopicType.Pubsub)));

        List<TopicMapping.MappingEntry> topicMappingsWithDestinationAsLocalMqtt =
                mqttClient.getTopicMappingsWithDestinationAsLocalMqtt();
        MatcherAssert.assertThat(topicMappingsWithDestinationAsLocalMqtt, Matchers.hasSize(1));
        MatcherAssert.assertThat(topicMappingsWithDestinationAsLocalMqtt, Matchers.containsInAnyOrder(
                new TopicMapping.MappingEntry("mqtt/topic3", TopicMapping.TopicType.Pubsub, "/test/pubsub/topic2",
                        TopicMapping.TopicType.LocalMqtt)));

        topicArgumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockMqttClient, times(1)).unsubscribe(topicArgumentCaptor.capture());
        MatcherAssert.assertThat(topicArgumentCaptor.getValue(), Matchers.is(Matchers.equalTo("mqtt/topic2")));
    }

    @Test
    void GIVEN_mqtt_client_and_mapping_populated_WHEN_receive_mqtt_message_THEN_routed_to_message_bridge()
            throws Exception {
        TopicMapping mapping = new TopicMapping();
        mapping.updateMapping("[\n"
                + "  {\"SourceTopic\": \"mqtt/topic\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/cloud/topic\", \"DestTopicType\": \"IotCore\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic2\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/pubsub/topic\", \"DestTopicType\": \"Pubsub\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic3\", \"SourceTopicType\": \"IotCore\", \"DestTopic\": "
                + "\"/test/cloud/topic2\", \"DestTopicType\": \"LocalMqtt\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic4\", \"SourceTopicType\": \"Pubsub\", \"DestTopic\": "
                + "\"/test/cloud/topic2\", \"DestTopicType\": \"LocalMqtt\"}\n" + "]");
        when(mockTopics.findOrDefault(any(), eq(MQTTClient.BROKER_URI_KEY))).thenReturn(SERVER_URI);
        when(mockTopics.findOrDefault(any(), eq(MQTTClient.CLIENT_ID_KEY))).thenReturn(CLIENT_ID);
        doNothing().when(mockMqttClient).connect(any(MqttConnectOptions.class));
        ArgumentCaptor<MqttCallback> mqttCallbackArgumentCaptor = ArgumentCaptor.forClass(MqttCallback.class);
        doNothing().when(mockMqttClient).setCallback(mqttCallbackArgumentCaptor.capture());
        MQTTClient mqttClient = new MQTTClient(mockTopics, mapping, mockMessageBridge, mockMqttClient);
        mqttClient.start();

        byte[] messageOnTopic1 = "message from topic mqtt/topic".getBytes();
        byte[] messageOnTopic2 = "message from topic mqtt/topic2".getBytes();
        MqttCallback mqttCallback = mqttCallbackArgumentCaptor.getValue();
        mqttCallback.messageArrived("mqtt/topic", new MqttMessage(messageOnTopic1));
        mqttCallback.messageArrived("mqtt/topic2", new MqttMessage(messageOnTopic2));

        // Also simulate a message which is not in the mapping
        mqttCallback.messageArrived("mqtt/topic/not/in/mapping",
                new MqttMessage("message from topic mqtt/topic/not/in/mapping".getBytes()));

        ArgumentCaptor<Message> messageCapture = ArgumentCaptor.forClass(Message.class);
        ArgumentCaptor<TopicMapping.TopicType> topicTypeArgumentCaptor =
                ArgumentCaptor.forClass(TopicMapping.TopicType.class);
        verify(mockMessageBridge, times(2)).notifyMessage(messageCapture.capture(), topicTypeArgumentCaptor.capture());

        MatcherAssert.assertThat(messageCapture.getAllValues().get(0).getTopic(),
                Matchers.is(Matchers.equalTo("mqtt/topic")));
        Assertions.assertArrayEquals(messageOnTopic1, messageCapture.getAllValues().get(0).getPayload());

        MatcherAssert.assertThat(messageCapture.getAllValues().get(1).getTopic(),
                Matchers.is(Matchers.equalTo("mqtt/topic2")));
        Assertions.assertArrayEquals(messageOnTopic2, messageCapture.getAllValues().get(1).getPayload());

        MatcherAssert.assertThat(topicTypeArgumentCaptor.getAllValues(),
                Matchers.contains(TopicMapping.TopicType.LocalMqtt, TopicMapping.TopicType.LocalMqtt));
    }

    @Test
    void GIVEN_mqtt_client_and_mapping_populated_WHEN_receive_pubsub_and_iotcore_message_THEN_routed_to_mqtt_broker()
            throws Exception {
        TopicMapping mapping = new TopicMapping();
        mapping.updateMapping("[\n"
                + "  {\"SourceTopic\": \"mqtt/topic\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/cloud/topic\", \"DestTopicType\": \"IotCore\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic2\", \"SourceTopicType\": \"LocalMqtt\", \"DestTopic\": \"/test/pubsub/topic\", \"DestTopicType\": \"Pubsub\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic3\", \"SourceTopicType\": \"IotCore\", \"DestTopic\": "
                + "\"/test/cloud/topic2\", \"DestTopicType\": \"LocalMqtt\"},\n"
                + "  {\"SourceTopic\": \"mqtt/topic4\", \"SourceTopicType\": \"Pubsub\", \"DestTopic\": "
                + "\"/test/cloud/topic3\", \"DestTopicType\": \"LocalMqtt\"}\n" + "]");
        when(mockTopics.findOrDefault(any(), eq(MQTTClient.BROKER_URI_KEY))).thenReturn(SERVER_URI);
        when(mockTopics.findOrDefault(any(), eq(MQTTClient.CLIENT_ID_KEY))).thenReturn(CLIENT_ID);
        doNothing().when(mockMqttClient).connect(any(MqttConnectOptions.class));
        ArgumentCaptor<MqttCallback> mqttCallbackArgumentCaptor = ArgumentCaptor.forClass(MqttCallback.class);
        doNothing().when(mockMqttClient).setCallback(mqttCallbackArgumentCaptor.capture());
        MQTTClient mqttClient = new MQTTClient(mockTopics, mapping, mockMessageBridge, mockMqttClient);
        mqttClient.start();

        ArgumentCaptor<MessageBridge.MessageListener> messageListenerArgumentCaptor =
                ArgumentCaptor.forClass(MessageBridge.MessageListener.class);
        ArgumentCaptor<TopicMapping.TopicType> sourceTypeArgumentCaptor =
                ArgumentCaptor.forClass(TopicMapping.TopicType.class);
        verify(mockMessageBridge, times(2))
                .addListener(messageListenerArgumentCaptor.capture(), sourceTypeArgumentCaptor.capture());

        MessageBridge.MessageListener iotCoreMessageListener;
        MessageBridge.MessageListener pubsubMessageListener;
        if (sourceTypeArgumentCaptor.getAllValues().get(0).equals(TopicMapping.TopicType.IotCore)) {
            iotCoreMessageListener = messageListenerArgumentCaptor.getAllValues().get(0);
            pubsubMessageListener = messageListenerArgumentCaptor.getAllValues().get(1);
        } else {
            pubsubMessageListener = messageListenerArgumentCaptor.getAllValues().get(0);
            iotCoreMessageListener = messageListenerArgumentCaptor.getAllValues().get(1);
        }

        byte[] messageFromPubsub = "message from pusub".getBytes();
        byte[] messageFromIotCore = "message from iotcore".getBytes();

        iotCoreMessageListener
                .onMessage(TopicMapping.TopicType.IotCore, new Message("mqtt/topic3", messageFromIotCore));
        pubsubMessageListener.onMessage(TopicMapping.TopicType.Pubsub, new Message("mqtt/topic4", messageFromPubsub));

        // Also simulate messages we are not interested in
        iotCoreMessageListener.onMessage(TopicMapping.TopicType.IotCore,
                new Message("mqtt/topic/not/in/mapping", messageFromIotCore));
        pubsubMessageListener
                .onMessage(TopicMapping.TopicType.Pubsub, new Message("mqtt/topic2/not/in/mapping", messageFromPubsub));

        ArgumentCaptor<MqttMessage> messageCapture = ArgumentCaptor.forClass(MqttMessage.class);
        ArgumentCaptor<String> topicStringArgumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockMqttClient, times(2)).publish(topicStringArgumentCaptor.capture(), messageCapture.capture());

        MatcherAssert.assertThat(topicStringArgumentCaptor.getAllValues().get(0),
                Matchers.is(Matchers.equalTo("/test/cloud/topic2")));
        Assertions.assertArrayEquals(messageFromIotCore, messageCapture.getAllValues().get(0).getPayload());

        MatcherAssert.assertThat(topicStringArgumentCaptor.getAllValues().get(1),
                Matchers.is(Matchers.equalTo("/test/cloud/topic3")));
        Assertions.assertArrayEquals(messageFromPubsub, messageCapture.getAllValues().get(1).getPayload());
    }
}
