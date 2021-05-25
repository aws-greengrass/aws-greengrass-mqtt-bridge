/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge.clients;

import com.aws.greengrass.componentmanager.KernelConfigResolver;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.mqttbridge.Message;
import com.aws.greengrass.mqttbridge.auth.MQTTClientKeyStore;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import javax.net.ssl.SSLSocketFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith({MockitoExtension.class, GGExtension.class})
public class MQTTClientTest {

    private static final String SERVER_URI = "ssl://localhost:8883";
    private static final String CLIENT_ID = "mqttBridge";

    @Mock
    private Topics mockTopics;

    @Mock
    private MqttClient mockMqttClient;

    private FakeMqttClient fakeMqttClient;

    @Mock
    private MQTTClientKeyStore mockMqttClientKeyStore;

    ScheduledExecutorService ses = new ScheduledThreadPoolExecutor(1);

    @BeforeEach
    void setup() {
        // TODO: Are these needed or can we use the class defaults? Maybe just create a real Topics?
        when(mockTopics
                .findOrDefault(any(), eq(KernelConfigResolver.CONFIGURATION_CONFIG_KEY), eq(MQTTClient.BROKER_URI_KEY)))
                .thenReturn(SERVER_URI);
        when(mockTopics
                .findOrDefault(any(), eq(KernelConfigResolver.CONFIGURATION_CONFIG_KEY), eq(MQTTClient.CLIENT_ID_KEY)))
                .thenReturn(CLIENT_ID);

        fakeMqttClient = new FakeMqttClient("clientId");
        lenient().when(mockMqttClient.isConnected()).thenReturn(true); // TODO: remove
    }

    @Test
    void GIVEN_mqttClient_WHEN_start_THEN_clientConnects() {
        MQTTClient mqttClient = new MQTTClient(mockTopics, mockMqttClientKeyStore, ses, fakeMqttClient);
        mqttClient.start();
        fakeMqttClient.waitForConnect(1000);

        assertThat(fakeMqttClient.isConnected(), is(true));
    }

    @Test
    void GIVEN_subscribedMqttClient_WHEN_stop_THEN_clientUnsubscribes() throws Exception {
        MQTTClient mqttClient = new MQTTClient(mockTopics, mockMqttClientKeyStore, ses, fakeMqttClient);
        mqttClient.start();
        fakeMqttClient.waitForConnect(1000);

        Set<String> topics = new HashSet<>();
        topics.add("mqtt/topic");
        topics.add("mqtt/topic2");
        mqttClient.updateSubscriptions(topics, message -> {
        });

        mqttClient.stop();

        List<String> subscriptions = fakeMqttClient.getSubscriptionTopics();
        assertThat(fakeMqttClient.isConnected(), is(false));
        assertThat(subscriptions, hasSize(0));
    }

    @Test
    void GIVEN_subscribedMqttClient_WHEN_updateSubscriptions_THEN_subscriptionsUpdated() {
        MQTTClient mqttClient = new MQTTClient(mockTopics, mockMqttClientKeyStore, ses, fakeMqttClient);
        mqttClient.start();
        fakeMqttClient.waitForConnect(1000);

        // Initial subscriptions
        Set<String> topics = new HashSet<>();
        topics.add("mqtt/topic");
        topics.add("mqtt/topic2");
        mqttClient.updateSubscriptions(topics, message -> {
        });
        List<String> subscriptions = fakeMqttClient.getSubscriptionTopics();
        assertThat(subscriptions, containsInAnyOrder("mqtt/topic", "mqtt/topic2"));

        // Add new topics
        topics.add("mqtt/topic3");
        mqttClient.updateSubscriptions(topics, message -> {
        });
        mqttClient.updateSubscriptions(topics, message -> {
        });
        subscriptions = fakeMqttClient.getSubscriptionTopics();
        assertThat(subscriptions, containsInAnyOrder("mqtt/topic", "mqtt/topic2", "mqtt/topic3"));

        // Replace topics
        topics.clear();
        topics.add("mqtt/topic");
        topics.add("mqtt/topic2/changed");
        topics.add("mqtt/topic3/changed");
        mqttClient.updateSubscriptions(topics, message -> {
        });
        subscriptions = fakeMqttClient.getSubscriptionTopics();
        assertThat(subscriptions, containsInAnyOrder("mqtt/topic", "mqtt/topic2/changed", "mqtt/topic3/changed"));

        // Remove topics
        topics.remove("mqtt/topic");
        topics.remove("mqtt/topic3/changed");
        mqttClient.updateSubscriptions(topics, message -> {
        });
        subscriptions = fakeMqttClient.getSubscriptionTopics();
        assertThat(subscriptions, containsInAnyOrder("mqtt/topic2/changed"));

        topics.clear();
        mqttClient.updateSubscriptions(topics, message -> {
        });
        subscriptions = fakeMqttClient.getSubscriptionTopics();
        assertThat(subscriptions, hasSize(0));
    }

    @Test
    void GIVEN_subscribedMqttClient_WHEN_mqttMessageReceived_THEN_messageRoutedToHandler() throws Exception {
        MQTTClient mqttClient = new MQTTClient(mockTopics, mockMqttClientKeyStore, ses, fakeMqttClient);
        mqttClient.start();
        fakeMqttClient.waitForConnect(1000);

        String t1 = "mqtt/topic";
        String t2 = "mqtt/topic2";
        byte[] m1 = "message from topic mqtt/topic".getBytes();
        byte[] m2 = "message from topic mqtt/topic2".getBytes();

        List<Message> receivedMessages = new ArrayList<>();

        // Initial subscriptions
        Set<String> topics = new HashSet<>();
        topics.add(t1);
        topics.add(t2);
        mqttClient.updateSubscriptions(topics, message -> {
            receivedMessages.add(message);
        });

        fakeMqttClient.injectMessage(t1, new MqttMessage(m1));
        fakeMqttClient.injectMessage(t2, new MqttMessage(m2));

        assertThat(receivedMessages, contains(new Message(t1, m1), new Message(t2, m2)));
    }

    @Test
    void GIVEN_mqttClient_WHEN_publish_THEN_routedToBroker() throws Exception {
        MQTTClient mqttClient = new MQTTClient(mockTopics, mockMqttClientKeyStore, ses, mockMqttClient);

        Set<String> topics = new HashSet<>();
        topics.add("mqtt/topic");
        topics.add("mqtt/topic2");
        mqttClient.updateSubscriptions(topics, message -> {
        });

        byte[] messageFromPubsub = "message from pusub".getBytes();
        byte[] messageFromIotCore = "message from iotcore".getBytes();

        mqttClient.publish(new Message("mapped/topic/from/pubsub", messageFromPubsub));
        mqttClient.publish(new Message("mapped/topic/from/iotcore", messageFromIotCore));

        ArgumentCaptor<MqttMessage> messageCapture = ArgumentCaptor.forClass(MqttMessage.class);
        ArgumentCaptor<String> topicStringArgumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockMqttClient, times(2)).publish(topicStringArgumentCaptor.capture(), messageCapture.capture());

        assertThat(topicStringArgumentCaptor.getAllValues().get(0), is(Matchers.equalTo("mapped/topic/from/pubsub")));
        Assertions.assertArrayEquals(messageFromPubsub, messageCapture.getAllValues().get(0).getPayload());

        assertThat(topicStringArgumentCaptor.getAllValues().get(1), is(Matchers.equalTo("mapped/topic/from/iotcore")));
        Assertions.assertArrayEquals(messageFromIotCore, messageCapture.getAllValues().get(1).getPayload());
    }

    @Test
    void GIVEN_mqttClient_WHEN_connectionLost_THEN_clientReconnectsAndResubscribes() throws Exception {
        MQTTClient mqttClient = new MQTTClient(mockTopics, mockMqttClientKeyStore, ses, fakeMqttClient);
        mqttClient.start();
        fakeMqttClient.waitForConnect(1000);

        Set<String> topics = new HashSet<>();
        topics.add("mqtt/topic");
        topics.add("mqtt/topic2");
        mqttClient.updateSubscriptions(topics, message -> {
        });

        fakeMqttClient.injectConnectionLoss();

        assertThat(fakeMqttClient.isConnected(), is(true));
        assertThat(fakeMqttClient.getConnectCount(), is(2));
        assertThat(fakeMqttClient.getSubscriptionTopics(), containsInAnyOrder("mqtt/topic", "mqtt/topic2"));
    }

    @Test
    void GIVEN_mqttClient_WHEN_reset_THEN_connectsWithUpdatedSslContext() throws Exception {
        MQTTClientKeyStore mockKeyStore = mock(MQTTClientKeyStore.class);
        MQTTClient mqttClient = new MQTTClient(mockTopics, mockKeyStore, ses, fakeMqttClient);
        mqttClient.start();
        fakeMqttClient.waitForConnect(1000);

        SSLSocketFactory mockSocketFactory = mock(SSLSocketFactory.class);
        when(mockKeyStore.getSSLSocketFactory()).thenReturn(mockSocketFactory);

        // This code assumes reset synchronously disconnects. This will need to be revisited if
        // this assumption changes and this test starts failing
        mqttClient.reset();
        fakeMqttClient.waitForConnect(1000);

        assertThat(fakeMqttClient.getConnectOptions().getSocketFactory(), is(mockSocketFactory));
        assertThat(fakeMqttClient.getConnectCount(), is(2));
    }
}
