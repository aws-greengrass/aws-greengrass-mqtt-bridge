/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge.clients;

import com.aws.greengrass.mqttbridge.Message;
import com.aws.greengrass.mqttbridge.auth.MQTTClientKeyStore;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.ssl.SSLSocketFactory;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@ExtendWith({MockitoExtension.class, GGExtension.class})
public class MQTTClientTest {

    private static final URI ENCRYPTED_URI = URI.create("ssl://localhost:8883");
    private static final String CLIENT_ID = "mqtt-bridge-1234";

    private FakeMqttClient fakeMqttClient;

    @Mock
    private MQTTClientKeyStore mockMqttClientKeyStore;

    private ScheduledExecutorService ses;

    @BeforeEach
    void setup() {
        fakeMqttClient = new FakeMqttClient(CLIENT_ID);
        ses = new ScheduledThreadPoolExecutor(1);
    }

    @AfterEach
    void tearDown() {
        ses.shutdownNow();
    }

    @Test
    void GIVEN_mqttClient_WHEN_start_THEN_clientConnects() {
        MQTTClient mqttClient = new MQTTClient(ENCRYPTED_URI, CLIENT_ID, mockMqttClientKeyStore, ses, fakeMqttClient);
        mqttClient.start();
        fakeMqttClient.waitForConnect(1000);

        assertThat(fakeMqttClient.isConnected(), is(true));
    }

    @Test
    void GIVEN_subscribedMqttClient_WHEN_stop_THEN_clientUnsubscribes() {
        MQTTClient mqttClient = new MQTTClient(ENCRYPTED_URI, CLIENT_ID, mockMqttClientKeyStore, ses, fakeMqttClient);
        mqttClient.start();
        fakeMqttClient.waitForConnect(1000);

        Set<String> topics = new HashSet<>();
        topics.add("mqtt/topic");
        topics.add("mqtt/topic2");
        mqttClient.updateSubscriptions(topics, message -> {
        });

        List<String> subscriptions = fakeMqttClient.getSubscriptionTopics();
        assertThat(subscriptions, containsInAnyOrder("mqtt/topic", "mqtt/topic2"));

        mqttClient.stop();

        subscriptions = fakeMqttClient.getSubscriptionTopics();
        assertThat(fakeMqttClient.isConnected(), is(false));
        assertThat(subscriptions, hasSize(0));
    }

    @Test
    void GIVEN_subscribedMqttClient_WHEN_updateSubscriptions_THEN_subscriptionsUpdated() {
        MQTTClient mqttClient = new MQTTClient(ENCRYPTED_URI, CLIENT_ID, mockMqttClientKeyStore, ses, fakeMqttClient);
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
        MQTTClient mqttClient = new MQTTClient(ENCRYPTED_URI, CLIENT_ID, mockMqttClientKeyStore, ses, fakeMqttClient);
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
        MQTTClient mqttClient = new MQTTClient(ENCRYPTED_URI, CLIENT_ID, mockMqttClientKeyStore, ses, fakeMqttClient);
        mqttClient.start();
        fakeMqttClient.waitForConnect(1000);

        byte[] messageFromPubsub = "message from pusub".getBytes();
        byte[] messageFromIotCore = "message from iotcore".getBytes();

        mqttClient.publish(new Message("from/pubsub", messageFromPubsub));
        mqttClient.publish(new Message("from/iotcore", messageFromIotCore));

        List<FakeMqttClient.TopicMessagePair> publishedMessages = fakeMqttClient.getPublishedMessages();
        assertThat(publishedMessages.size(), is(2));
        assertThat(publishedMessages.get(0).getTopic(), equalTo("from/pubsub"));
        assertThat(publishedMessages.get(0).getMessage().getPayload(), equalTo(messageFromPubsub));
        assertThat(publishedMessages.get(1).getTopic(), equalTo("from/iotcore"));
        assertThat(publishedMessages.get(1).getMessage().getPayload(), equalTo(messageFromIotCore));
    }

    @Test
    void GIVEN_mqttClient_WHEN_connectionLost_THEN_clientReconnectsAndResubscribes() throws Exception {
        MQTTClient mqttClient = new MQTTClient(ENCRYPTED_URI, CLIENT_ID, mockMqttClientKeyStore, ses, fakeMqttClient);
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
    void GIVEN_mqttClient_WHEN_caRotates_THEN_connectsWithUpdatedSslContext() throws Exception {
        MQTTClientKeyStore mockKeyStore = mock(MQTTClientKeyStore.class);
        MQTTClient mqttClient = new MQTTClient(ENCRYPTED_URI, CLIENT_ID, mockKeyStore, ses, fakeMqttClient);
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

    @Test
    void GIVEN_mqttClient_WHEN_clientCertRotates_THEN_newCertIsUsedUponSubsequentReconnects() throws Exception {
        SSLSocketFactory mockSocketFactory1 = mock(SSLSocketFactory.class);
        SSLSocketFactory mockSocketFactory2 = mock(SSLSocketFactory.class);
        when(mockMqttClientKeyStore.getSSLSocketFactory()).thenReturn(mockSocketFactory1);

        MQTTClient mqttClient = new MQTTClient(ENCRYPTED_URI, CLIENT_ID, mockMqttClientKeyStore, ses, fakeMqttClient);
        mqttClient.start();
        fakeMqttClient.waitForConnect(1000);

        assertThat(fakeMqttClient.isConnected(), is(true));
        MqttConnectOptions connectOptions = fakeMqttClient.getConnectOptions();
        assertThat(connectOptions.getSocketFactory(), is(mockSocketFactory1));

        // Update socket factory and inject a connection loss
        when(mockMqttClientKeyStore.getSSLSocketFactory()).thenReturn(mockSocketFactory2);
        fakeMqttClient.injectConnectionLoss();

        assertThat(fakeMqttClient.isConnected(), is(true));
        connectOptions = fakeMqttClient.getConnectOptions();
        assertThat(connectOptions.getSocketFactory(), is(mockSocketFactory2));
    }
}
