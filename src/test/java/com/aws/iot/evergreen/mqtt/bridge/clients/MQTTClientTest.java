/* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0 */

package com.aws.iot.evergreen.mqtt.bridge.clients;

import com.aws.iot.evergreen.config.Topics;
import com.aws.iot.evergreen.mqtt.bridge.Message;
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

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith({MockitoExtension.class, EGExtension.class})
public class MQTTClientTest {

    private static final String SERVER_URI = "testUri";
    private static final String CLIENT_ID = "id";

    @Mock
    private Topics mockTopics;

    @Mock
    private MqttClient mockMqttClient;

    @Mock
    private Consumer<Message> mockMessageHandler;

    @Test
    void WHEN_call_mqtt_client_constructed_THEN_does_not_throw() {
        when(mockTopics.findOrDefault(any(), eq(MQTTClient.BROKER_URI_KEY))).thenReturn(SERVER_URI);
        when(mockTopics.findOrDefault(any(), eq(MQTTClient.CLIENT_ID_KEY))).thenReturn(CLIENT_ID);
        new MQTTClient(mockTopics, mockMqttClient);
    }

    @Test
    void GIVEN_mqtt_client_WHEN_call_start_THEN_connected_to_broker() throws Exception {
        when(mockTopics.findOrDefault(any(), eq(MQTTClient.BROKER_URI_KEY))).thenReturn(SERVER_URI);
        when(mockTopics.findOrDefault(any(), eq(MQTTClient.CLIENT_ID_KEY))).thenReturn(CLIENT_ID);
        doNothing().when(mockMqttClient).connect(any(MqttConnectOptions.class));
        doNothing().when(mockMqttClient).setCallback(any());
        MQTTClient mqttClient = new MQTTClient(mockTopics, mockMqttClient);
        mqttClient.start();
        verify(mockMqttClient, times(1)).connect(any());
        verify(mockMqttClient, times(1)).setCallback(any());
    }

    @Test
    void GIVEN_mqtt_client_WHEN_start_throws_exception_THEN_mqtt_client_exception_is_thrown() throws Exception {
        when(mockTopics.findOrDefault(any(), eq(MQTTClient.BROKER_URI_KEY))).thenReturn(SERVER_URI);
        when(mockTopics.findOrDefault(any(), eq(MQTTClient.CLIENT_ID_KEY))).thenReturn(CLIENT_ID);
        doThrow(new MqttException(0)).when(mockMqttClient).connect(any(MqttConnectOptions.class));
        MQTTClient mqttClient = new MQTTClient(mockTopics, mockMqttClient);
        Assertions.assertThrows(MQTTClientException.class, mqttClient::start);
    }

    @Test
    void GIVEN_mqtt_client_started_WHEN_update_subscriptions_THEN_topics_subscribed() throws Exception {
        MQTTClient mqttClient = new MQTTClient(mockTopics, mockMqttClient);
        Set<String> topics = new HashSet<>();
        topics.add("mqtt/topic");
        topics.add("mqtt/topic2");
        mqttClient.updateSubscriptions(topics, message -> {
        });

        ArgumentCaptor<String> topicArgumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockMqttClient, times(2)).subscribe(topicArgumentCaptor.capture());
        MatcherAssert.assertThat(topicArgumentCaptor.getAllValues(),
                Matchers.containsInAnyOrder("mqtt/topic", "mqtt/topic2"));

        MatcherAssert.assertThat(mqttClient.getSubscribedLocalMqttTopics(),
                Matchers.containsInAnyOrder("mqtt/topic", "mqtt/topic2"));
    }

    @Test
    void GIVEN_mqtt_client_with_subscriptions_WHEN_call_stop_THEN_topics_unsubscribed() throws Exception {
        MQTTClient mqttClient = new MQTTClient(mockTopics, mockMqttClient);
        Set<String> topics = new HashSet<>();
        topics.add("mqtt/topic");
        topics.add("mqtt/topic2");
        mqttClient.updateSubscriptions(topics, message -> {
        });

        mqttClient.stop();

        ArgumentCaptor<String> topicArgumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockMqttClient, times(2)).unsubscribe(topicArgumentCaptor.capture());
        MatcherAssert.assertThat(topicArgumentCaptor.getAllValues(),
                Matchers.containsInAnyOrder("mqtt/topic", "mqtt/topic2"));

        MatcherAssert.assertThat(mqttClient.getSubscribedLocalMqttTopics(), Matchers.hasSize(0));

        verify(mockMqttClient, times(1)).disconnect();
    }

    @Test
    void GIVEN_mqtt_client_with_subscriptions_WHEN_subscriptions_updated_THEN_subscriptions_updated() throws Exception {
        MQTTClient mqttClient = new MQTTClient(mockTopics, mockMqttClient);
        Set<String> topics = new HashSet<>();
        topics.add("mqtt/topic");
        topics.add("mqtt/topic2");
        mqttClient.updateSubscriptions(topics, message -> {
        });

        reset(mockMqttClient);

        topics.clear();
        topics.add("mqtt/topic");
        topics.add("mqtt/topic2/changed");
        topics.add("mqtt/topic3/added");
        mqttClient.updateSubscriptions(topics, message -> {
        });

        ArgumentCaptor<String> topicArgumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockMqttClient, times(2)).subscribe(topicArgumentCaptor.capture());
        MatcherAssert.assertThat(topicArgumentCaptor.getAllValues(),
                Matchers.containsInAnyOrder("mqtt/topic2/changed", "mqtt/topic3/added"));

        MatcherAssert.assertThat(mqttClient.getSubscribedLocalMqttTopics(), Matchers.hasSize(3));
        MatcherAssert.assertThat(mqttClient.getSubscribedLocalMqttTopics(),
                Matchers.containsInAnyOrder("mqtt/topic", "mqtt/topic2/changed", "mqtt/topic3/added"));

        topicArgumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockMqttClient, times(1)).unsubscribe(topicArgumentCaptor.capture());
        MatcherAssert.assertThat(topicArgumentCaptor.getValue(), Matchers.is(Matchers.equalTo("mqtt/topic2")));
    }

    @Test
    void GIVEN_mqtt_client_and_subscribed_WHEN_receive_mqtt_message_THEN_routed_to_message_handler() throws Exception {
        MQTTClient mqttClient = new MQTTClient(mockTopics, mockMqttClient);
        doNothing().when(mockMqttClient).connect(any(MqttConnectOptions.class));
        ArgumentCaptor<MqttCallback> mqttCallbackArgumentCaptor = ArgumentCaptor.forClass(MqttCallback.class);
        doNothing().when(mockMqttClient).setCallback(mqttCallbackArgumentCaptor.capture());

        mqttClient.start();
        Set<String> topics = new HashSet<>();
        topics.add("mqtt/topic");
        topics.add("mqtt/topic2");
        mqttClient.updateSubscriptions(topics, mockMessageHandler);

        byte[] messageOnTopic1 = "message from topic mqtt/topic".getBytes();
        byte[] messageOnTopic2 = "message from topic mqtt/topic2".getBytes();
        byte[] messageOnTopic3 = "message from topic mqtt/topic/not/in/mapping".getBytes();
        MqttCallback mqttCallback = mqttCallbackArgumentCaptor.getValue();
        mqttCallback.messageArrived("mqtt/topic", new MqttMessage(messageOnTopic1));
        mqttCallback.messageArrived("mqtt/topic2", new MqttMessage(messageOnTopic2));
        // Also simulate a message which is not in the mapping
        mqttCallback.messageArrived("mqtt/topic/not/in/mapping", new MqttMessage(messageOnTopic3));

        ArgumentCaptor<Message> messageCapture = ArgumentCaptor.forClass(Message.class);
        verify(mockMessageHandler, times(3)).accept(messageCapture.capture());

        MatcherAssert.assertThat(messageCapture.getAllValues().get(0).getTopic(),
                Matchers.is(Matchers.equalTo("mqtt/topic")));
        Assertions.assertArrayEquals(messageOnTopic1, messageCapture.getAllValues().get(0).getPayload());

        MatcherAssert.assertThat(messageCapture.getAllValues().get(1).getTopic(),
                Matchers.is(Matchers.equalTo("mqtt/topic2")));
        Assertions.assertArrayEquals(messageOnTopic2, messageCapture.getAllValues().get(1).getPayload());

        MatcherAssert.assertThat(messageCapture.getAllValues().get(2).getTopic(),
                Matchers.is(Matchers.equalTo("mqtt/topic/not/in/mapping")));
        Assertions.assertArrayEquals(messageOnTopic3, messageCapture.getAllValues().get(2).getPayload());
    }

    @Test
    void GIVEN_mqtt_client_and_subscribed_WHEN_published_message_THEN_routed_to_mqtt_broker() throws Exception {
        MQTTClient mqttClient = new MQTTClient(mockTopics, mockMqttClient);
        Set<String> topics = new HashSet<>();
        topics.add("mqtt/topic");
        topics.add("mqtt/topic2");
        mqttClient.updateSubscriptions(topics, mockMessageHandler);

        byte[] messageFromPubsub = "message from pusub".getBytes();
        byte[] messageFromIotCore = "message from iotcore".getBytes();

        mqttClient.publish(new Message("mapped/topic/from/pubsub", messageFromPubsub));
        mqttClient.publish(new Message("mapped/topic/from/iotcore", messageFromIotCore));

        ArgumentCaptor<MqttMessage> messageCapture = ArgumentCaptor.forClass(MqttMessage.class);
        ArgumentCaptor<String> topicStringArgumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockMqttClient, times(2)).publish(topicStringArgumentCaptor.capture(), messageCapture.capture());

        MatcherAssert.assertThat(topicStringArgumentCaptor.getAllValues().get(0),
                Matchers.is(Matchers.equalTo("mapped/topic/from/pubsub")));
        Assertions.assertArrayEquals(messageFromPubsub, messageCapture.getAllValues().get(0).getPayload());

        MatcherAssert.assertThat(topicStringArgumentCaptor.getAllValues().get(1),
                Matchers.is(Matchers.equalTo("mapped/topic/from/iotcore")));
        Assertions.assertArrayEquals(messageFromIotCore, messageCapture.getAllValues().get(1).getPayload());
    }
}
