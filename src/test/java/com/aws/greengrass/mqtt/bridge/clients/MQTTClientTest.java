/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.clients;

import com.aws.greengrass.mqtt.bridge.auth.MQTTClientKeyStore;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.TestUtils;
import com.aws.greengrass.util.CrashableSupplier;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.ssl.SSLSocketFactory;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.github.grantwest.eventually.EventuallyLambdaMatcher.eventuallyEval;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith({MockitoExtension.class, GGExtension.class})
@SuppressWarnings("PMD.CloseResource")
public class MQTTClientTest {

    private static final MQTTClient.Config CONFIG = MQTTClient.Config.builder()
            .clientId("mqtt-bridge-1234")
            .brokerUri(URI.create("ssl://localhost:8883"))
            .build();
    FakePahoMqtt3Client fakeMqttClient;
    private final CrashableSupplier<IMqttClient, MqttException> clientFactory = () -> {
                fakeMqttClient = new FakePahoMqtt3Client(CONFIG.getClientId(), CONFIG.getBrokerUri().toString());
                return fakeMqttClient;
    };

    @Mock
    private MQTTClientKeyStore mockMqttClientKeyStore;

    private final ExecutorService ses = TestUtils.synchronousExecutorService();

    @AfterEach
    void tearDown() {
        ses.shutdownNow();
    }

    @Test
    void GIVEN_mqttClient_WHEN_start_THEN_clientConnects() throws MessageClientException {
        MQTTClient mqttClient = new MQTTClient(CONFIG, mockMqttClientKeyStore, ses, clientFactory);
        mqttClient.start();
        fakeMqttClient.waitForConnect(1000);

        assertThat(fakeMqttClient.isConnected(), is(true));
    }

    @Test
    void GIVEN_subscribedMqttClient_WHEN_stop_THEN_clientUnsubscribes() throws MessageClientException {
        MQTTClient mqttClient = new MQTTClient(CONFIG, mockMqttClientKeyStore, ses, clientFactory);
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

        verify(mockMqttClientKeyStore).unsubscribeFromUpdates(any());
        subscriptions = fakeMqttClient.getSubscriptionTopics();
        assertThat(fakeMqttClient.isConnected(), is(false));
        assertThat(subscriptions, hasSize(0));
    }

    @Test
    void GIVEN_subscribedMqttClient_WHEN_updateSubscriptions_THEN_subscriptionsUpdated() throws MessageClientException {
        MQTTClient mqttClient = new MQTTClient(CONFIG, mockMqttClientKeyStore, ses, clientFactory);
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
        MQTTClient mqttClient = new MQTTClient(CONFIG, mockMqttClientKeyStore, ses, clientFactory);
        mqttClient.start();
        fakeMqttClient.waitForConnect(1000);

        String t1 = "mqtt/topic";
        String t2 = "mqtt/topic2";
        byte[] m1 = "message from topic mqtt/topic".getBytes();
        byte[] m2 = "message from topic mqtt/topic2".getBytes();

        List<com.aws.greengrass.mqtt.bridge.model.MqttMessage> receivedMessages = new ArrayList<>();

        // Initial subscriptions
        Set<String> topics = new HashSet<>();
        topics.add(t1);
        topics.add(t2);
        mqttClient.updateSubscriptions(topics, receivedMessages::add);

        fakeMqttClient.injectMessage(t1, new MqttMessage(m1));
        fakeMqttClient.injectMessage(t2, new MqttMessage(m2));

        assertThat(receivedMessages, contains(
                com.aws.greengrass.mqtt.bridge.model.MqttMessage.builder().topic(t1).payload(m1).build(),
                com.aws.greengrass.mqtt.bridge.model.MqttMessage.builder().topic(t2).payload(m2).build()));


    }

    @Test
    void GIVEN_mqttClient_WHEN_publish_THEN_routedToBroker() throws Exception {
        MQTTClient mqttClient = new MQTTClient(CONFIG, mockMqttClientKeyStore, ses, clientFactory);
        mqttClient.start();
        fakeMqttClient.waitForConnect(1000);

        byte[] messageFromPubsub = "message from pusub".getBytes();
        byte[] messageFromIotCore = "message from iotcore".getBytes();

        mqttClient.publish(com.aws.greengrass.mqtt.bridge.model.MqttMessage.builder().topic("from/pubsub").payload(messageFromPubsub).build());
        mqttClient.publish(com.aws.greengrass.mqtt.bridge.model.MqttMessage.builder().topic("from/iotcore").payload(messageFromIotCore).build());

        List<FakePahoMqtt3Client.TopicMessagePair> publishedMessages = fakeMqttClient.getPublishedMessages();
        assertThat(publishedMessages.size(), is(2));
        assertThat(publishedMessages.get(0).getTopic(), equalTo("from/pubsub"));
        assertThat(publishedMessages.get(0).getMessage().getPayload(), equalTo(messageFromPubsub));
        assertThat(publishedMessages.get(1).getTopic(), equalTo("from/iotcore"));
        assertThat(publishedMessages.get(1).getMessage().getPayload(), equalTo(messageFromIotCore));
    }

    @Test
    void GIVEN_mqttClient_WHEN_connectionLost_THEN_clientReconnectsAndResubscribes() throws Exception {
        MQTTClient mqttClient = new MQTTClient(CONFIG, mockMqttClientKeyStore, ses, clientFactory);
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
        MQTTClient mqttClient = new MQTTClient(CONFIG, mockKeyStore, ses, clientFactory);
        mqttClient.start();
        fakeMqttClient.waitForConnect(1000);

        SSLSocketFactory mockSocketFactory = mock(SSLSocketFactory.class);
        when(mockKeyStore.getSSLSocketFactory()).thenReturn(mockSocketFactory);

        // This code assumes reset synchronously disconnects. This will need to be revisited if
        // this assumption changes and this test starts failing
        mqttClient.reset(false);
        fakeMqttClient.waitForConnect(1000);

        assertThat(fakeMqttClient.getConnectOptions().getSocketFactory(), is(mockSocketFactory));
        assertThat(fakeMqttClient.getConnectCount(), is(2));
    }

    @Test
    void GIVEN_mqttClient_WHEN_clientCertRotates_THEN_newCertIsUsedUponSubsequentReconnects() throws Exception {
        SSLSocketFactory mockSocketFactory1 = mock(SSLSocketFactory.class);
        SSLSocketFactory mockSocketFactory2 = mock(SSLSocketFactory.class);
        when(mockMqttClientKeyStore.getSSLSocketFactory()).thenReturn(mockSocketFactory1);

        MQTTClient mqttClient = new MQTTClient(CONFIG, mockMqttClientKeyStore, ses, clientFactory);
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

    @ParameterizedTest
    @MethodSource("configChanges")
    void GIVEN_client_WHEN_config_changes_THEN_client_is_reset(Function<MQTTClient.Config, MQTTClient.Config> changeConfig, boolean resetExpected) throws MessageClientException, InterruptedException {
        MQTTClient mqttClient = new MQTTClient(CONFIG, mockMqttClientKeyStore, ses, clientFactory);
        mqttClient.start();
        mqttClient.applyConfig(changeConfig.apply(MQTTClient.Config.builder().build()));
        FakePahoMqtt3Client fakeMqttClient = this.fakeMqttClient;
        mqttClient.reset(true);
        if (resetExpected) {
            assertThat("client resets", () -> fakeMqttClient.disconnectCount, eventuallyEval(is(1)));
        } else {
            Thread.sleep(1000L);
            assertEquals(1, fakeMqttClient.connectCount);
            assertEquals(0, fakeMqttClient.disconnectCount);
        }
    }

    @Test
    void GIVEN_client_WHEN_config_does_not_change_THEN_client_is_not_reset() throws MessageClientException, InterruptedException {
        MQTTClient mqttClient = new MQTTClient(CONFIG, mockMqttClientKeyStore, ses, clientFactory);
        mqttClient.start();
        mqttClient.applyConfig(mqttClient.getConfig());
        Thread.sleep(1000L);
        assertEquals(1, fakeMqttClient.connectCount);
        assertEquals(0, fakeMqttClient.disconnectCount);
    }

    @SuppressWarnings("PMD.UnusedPrivateMethod")
    private static Stream<Arguments> configChanges() {
        Function<MQTTClient.Config, MQTTClient.Config> brokerUriChanges = config -> {
            try {
                return config.toBuilder().brokerUri(new URI("tcp://0.0.0.0:1883")).build();
            } catch (URISyntaxException e) {
                fail(e);
                return null;
            }
        };
        Function<MQTTClient.Config, MQTTClient.Config> clientIdChanges = config -> config.toBuilder().clientId("newClientId").build();

        return Stream.of(
                Arguments.of(brokerUriChanges, true),
                Arguments.of(clientIdChanges, true)
        );
    }
}
