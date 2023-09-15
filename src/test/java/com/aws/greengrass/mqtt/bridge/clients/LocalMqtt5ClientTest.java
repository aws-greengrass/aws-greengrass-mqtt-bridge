/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.clients;

import com.aws.greengrass.mqtt.bridge.BridgeConfig;
import com.aws.greengrass.mqtt.bridge.auth.MQTTClientKeyStore;
import com.aws.greengrass.mqtt.bridge.model.Mqtt5RouteOptions;
import com.aws.greengrass.mqtt.bridge.model.MqttMessage;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.crt.CrtRuntimeException;
import software.amazon.awssdk.crt.mqtt5.Mqtt5Client;
import software.amazon.awssdk.crt.mqtt5.Mqtt5ClientOptions;
import software.amazon.awssdk.crt.mqtt5.OnAttemptingConnectReturn;
import software.amazon.awssdk.crt.mqtt5.OnConnectionFailureReturn;
import software.amazon.awssdk.crt.mqtt5.OnConnectionSuccessReturn;
import software.amazon.awssdk.crt.mqtt5.OnDisconnectionReturn;
import software.amazon.awssdk.crt.mqtt5.OnStoppedReturn;
import software.amazon.awssdk.crt.mqtt5.packets.ConnAckPacket;
import software.amazon.awssdk.crt.mqtt5.packets.DisconnectPacket;
import software.amazon.awssdk.crt.mqtt5.packets.PubAckPacket;
import software.amazon.awssdk.crt.mqtt5.packets.PublishPacket;
import software.amazon.awssdk.crt.mqtt5.packets.SubAckPacket;
import software.amazon.awssdk.crt.mqtt5.packets.SubscribePacket;
import software.amazon.awssdk.crt.mqtt5.packets.UnsubAckPacket;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static com.github.grantwest.eventually.EventuallyLambdaMatcher.eventuallyEval;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class LocalMqtt5ClientTest {

    ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);
    ExecutorService executorService = TestUtils.synchronousExecutorService();
    Mqtt5ClientOptions.LifecycleEvents lifecycleEvents;
    MockMqtt5Client mockMqtt5Client;

    LocalMqtt5Client client;

    @BeforeEach
    void setUp() throws MessageClientException {
        createLocalMqtt5Client();
        client.start();
    }

    @AfterEach
    void tearDown() {
        client.stop();
        ses.shutdownNow();
    }

    @Test
    void GIVEN_client_WHEN_client_fails_to_start_during_resetTHEN_retry(ExtensionContext context) {
        ignoreExceptionOfType(context, MessageClientException.class);
        ignoreExceptionOfType(context, CrtRuntimeException.class);
        mockMqtt5Client.failToStart(2);
        client.reset();
        assertThat("client disconnects", () -> client.getClient().getIsConnected(), eventuallyEval(is(false)));
        assertThat("client reconnects", () -> client.getClient().getIsConnected(), eventuallyEval(is(true)));
    }

    @Test
    void GIVEN_client_WHEN_unable_create_client_during_reset_THEN_retry(ExtensionContext context) {
        ignoreExceptionOfType(context, MessageClientException.class);
        ignoreExceptionOfType(context, CrtRuntimeException.class);
        AtomicBoolean failed = new AtomicBoolean();
        client.setClientSupplier(() -> {
            if (failed.compareAndSet(false, true)) {
                throw new MessageClientException("");
            }
            return mockMqtt5Client.getClient();
        });
        client.reset();
        assertThat("client disconnects", () -> client.getClient().getIsConnected(), eventuallyEval(is(false)));
        assertThat("client reconnects", () -> client.getClient().getIsConnected(), eventuallyEval(is(true)));
    }

    @Test
    void GIVEN_client_WHEN_publish_on_nolocal_route_THEN_no_publish_occurs() throws Exception {
        Map<String, Mqtt5RouteOptions> routeOptions = new HashMap<>();
        routeOptions.put("iotcore/topic", Mqtt5RouteOptions.builder().noLocal(true).build());

        createLocalMqtt5ClientWithMqtt5Options(routeOptions);

        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");

        Set<String> topicsReceived = ConcurrentHashMap.newKeySet();
        client.updateSubscriptions(topics, m -> topicsReceived.add(m.getTopic()));

        // verify subscriptions were made
        assertThat("subscribed topics local client", () -> client.getSubscribedLocalMqttTopics(), eventuallyEval(is(topics)));
        assertThat("subscribed topics mock client", this::getMockSubscriptions, eventuallyEval(is(topics)));

        client.publish(MqttMessage.builder().topic("iotcore/topic").payload("message1".getBytes()).build());
        client.publish(MqttMessage.builder().topic("iotcore/topic2").payload("message2".getBytes()).build());

        Set<String> expectedInvokedHandlers = new HashSet<>();
        expectedInvokedHandlers.add("iotcore/topic2");
        assertThat("messages published", () -> mockMqtt5Client.getPublished().stream().map(PublishPacket::getTopic).collect(Collectors.toSet()), eventuallyEval(is(topics)));
        assertThat("handlers invoked", () -> topicsReceived, eventuallyEval(is(expectedInvokedHandlers)));
    }

    @Test
    void GIVEN_client_WHEN_port_is_missing_THEN_succeeds() throws Exception {
        client.stop();
        client = new LocalMqtt5Client(URI.create("tcp://localhost"),
                "test-client",
                BridgeConfig.DEFAULT_SESSION_EXPIRY_INTERVAL,
                BridgeConfig.DEFAULT_MAXIMUM_PACKET_SIZE,
                BridgeConfig.DEFAULT_RECEIVE_MAXIMUM,
                1L,
                1L,
                1L,
                0L,
                1L,
                1L,
                Collections.emptyMap(),
                mock(MQTTClientKeyStore.class),
                executorService,
                null);
    }

    @Test
    void GIVEN_client_WHEN_fail_to_connect_THEN_connection_failure() throws MessageClientException {
        client.stop();
        createLocalMqtt5Client();

        mockMqtt5Client.nextConnectReasonCode.add(ConnAckPacket.ConnectReasonCode.UNSPECIFIED_ERROR);
        client.start();

        verify(lifecycleEvents, timeout(5000L).times(1)).onConnectionFailure(any(), any());
    }

    @Test
    void GIVEN_client_with_no_subscriptions_WHEN_update_subscriptions_THEN_topics_subscribed() {
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");

        client.updateSubscriptions(topics, message -> {});

        assertThat("subscribed topics local client", () -> client.getSubscribedLocalMqttTopics(), eventuallyEval(is(topics)));
        assertThat("subscribed topics mock client", this::getMockSubscriptions, eventuallyEval(is(topics)));
    }

    @Test
    void GIVEN_client_WHEN_subscription_fails_THEN_no_topics_subscribed() throws RetryableMqttOperationException {
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/failed");
        topics.add("iotcore/topic2");
        LocalMqtt5Client clientSpy = spy(client);

        mockMqtt5Client.nextSubAckReasonCode.add("iotcore/failed", SubAckPacket.SubAckReasonCode.TOPIC_FILTER_INVALID);
        clientSpy.updateSubscriptions(topics, message -> {});

        Set<String> expectedTopics = new HashSet<>();
        expectedTopics.add("iotcore/topic2");

        assertThat("subscribed topics local client", clientSpy::getSubscribedLocalMqttTopics,
                eventuallyEval(is(expectedTopics)));
        assertThat("subscribed topics mock client", this::getMockSubscriptions, eventuallyEval(is(expectedTopics)));
        // verify that the subscription was not retried
        verify(clientSpy, times(1)).subscribe("iotcore/failed");
    }

    @Test
    void GIVEN_client_WHEN_subscription_fails_from_execution_exception_THEN_no_topics_subscribed(ExtensionContext context) {
        ignoreExceptionOfType(context, RuntimeException.class);

        Set<String> topics = new HashSet<>();
        topics.add("iotcore/failed");
        topics.add("iotcore/topic2");

        mockMqtt5Client.throwExceptions = true;
        client.updateSubscriptions(topics, message -> {});

        assertTrue(client.getSubscribedLocalMqttTopics().isEmpty());
        assertTrue(getMockSubscriptions().isEmpty());
    }

    @Test
    void GIVEN_offline_client_with_no_subscriptions_WHEN_update_subscriptions_THEN_subscribe_once_online() {
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");

        mockMqtt5Client.offline(DisconnectPacket.DisconnectReasonCode.KEEP_ALIVE_TIMEOUT);
        client.updateSubscriptions(topics, message -> {});

        // no topics subscribed
        assertThat("to subscribe topics", () -> client.getToSubscribeLocalMqttTopics(), eventuallyEval(is(topics)));
        assertTrue(client.getSubscribedLocalMqttTopics().isEmpty());
        assertTrue(mockMqtt5Client.getSubscriptions().isEmpty());

        mockMqtt5Client.online();

        // verify subscriptions were made
        assertThat("subscribed topics local client", () -> client.getSubscribedLocalMqttTopics(), eventuallyEval(is(topics)));
        assertThat("subscribed topics mock client", this::getMockSubscriptions, eventuallyEval(is(topics)));
    }

    @Test
    void GIVEN_client_with_subscriptions_WHEN_stopped_THEN_topics_not_unsubscribed() {
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");

        client.updateSubscriptions(topics, message -> {});

        // verify subscriptions were made
        assertThat("subscribed topics local client", () -> client.getSubscribedLocalMqttTopics(), eventuallyEval(is(topics)));
        assertThat("subscribed topics mock client", this::getMockSubscriptions, eventuallyEval(is(topics)));

        client.stop();

        assertThat("iot core client unsubscribed", () -> client.getSubscribedLocalMqttTopics().isEmpty(), eventuallyEval(is(false)));
    }

    @Test
    void GIVEN_client_with_subscriptions_WHEN_topic_changed_and_unsubscribe_fails_THEN_topic_still_there()
            throws RetryableMqttOperationException {
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");
        LocalMqtt5Client clientSpy = spy(client);

        clientSpy.updateSubscriptions(topics, message -> {});

        // verify subscriptions were made
        assertThat("subscribed topics local client", clientSpy::getSubscribedLocalMqttTopics,
                eventuallyEval(is(topics)));
        assertThat("subscribed topics mock client", this::getMockSubscriptions, eventuallyEval(is(topics)));

        topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2/changed");

        mockMqtt5Client.nextUnsubAckReasonCode.add("iotcore/topic2",
                UnsubAckPacket.UnsubAckReasonCode.TOPIC_FILTER_INVALID);

        clientSpy.updateSubscriptions(topics, message -> {});

        Set<String> expectedSubscriptions = new HashSet<>(topics);
        expectedSubscriptions.add("iotcore/topic2");

        // verify subscriptions were made
        assertThat("subscribed topics local client", clientSpy::getSubscribedLocalMqttTopics,
                eventuallyEval(is(expectedSubscriptions)));
        assertThat("subscribed topics mock client", this::getMockSubscriptions,
                eventuallyEval(is(expectedSubscriptions)));
        verify(clientSpy, times(1)).unsubscribe("iotcore/topic2");
    }

    @Test
    void GIVEN_client_with_subscriptions_WHEN_topic_changed_fails_to_execution_exception_THEN_topic_still_there(ExtensionContext context) {
        ignoreExceptionOfType(context, RuntimeException.class);

        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");

        client.updateSubscriptions(topics, message -> {});

        // verify subscriptions were made
        assertThat("subscribed topics local client", () -> client.getSubscribedLocalMqttTopics(), eventuallyEval(is(topics)));
        assertThat("subscribed topics mock client", this::getMockSubscriptions, eventuallyEval(is(topics)));

        Set<String> updatedTopics = new HashSet<>();
        updatedTopics.add("iotcore/topic");
        updatedTopics.add("iotcore/topic2/changed");

        mockMqtt5Client.throwExceptions = true;

        client.updateSubscriptions(updatedTopics, message -> {});

        // verify no changes made
        assertThat("subscribed topics local client", () -> client.getSubscribedLocalMqttTopics(), eventuallyEval(is(topics)));
        assertThat("subscribed topics mock client", this::getMockSubscriptions, eventuallyEval(is(topics)));
    }

    @Test
    void GIVEN_client_with_subscriptions_WHEN_new_topic_added_THEN_subscription_made() {
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");

        client.updateSubscriptions(topics, message -> {});

        // verify subscriptions were made
        assertThat("subscribed topics local client", () -> client.getSubscribedLocalMqttTopics(), eventuallyEval(is(topics)));
        assertThat("subscribed topics mock client", this::getMockSubscriptions, eventuallyEval(is(topics)));

        topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2/changed");
        topics.add("iotcore/topic3/added");

        client.updateSubscriptions(topics, message -> {});

        // verify subscriptions were made
        assertThat("subscribed topics local client", () -> client.getSubscribedLocalMqttTopics(), eventuallyEval(is(topics)));
        assertThat("subscribed topics mock client", this::getMockSubscriptions, eventuallyEval(is(topics)));
    }

    @Test
    void GIVEN_client_with_subscriptions_WHEN_message_published_THEN_message_handler_invoked() throws Exception {
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");

        Set<String> topicsReceived = ConcurrentHashMap.newKeySet();
        client.updateSubscriptions(topics, m -> topicsReceived.add(m.getTopic()));

        // verify subscriptions were made
        assertThat("subscribed topics local client", () -> client.getSubscribedLocalMqttTopics(), eventuallyEval(is(topics)));
        assertThat("subscribed topics mock client", this::getMockSubscriptions, eventuallyEval(is(topics)));

        client.publish(MqttMessage.builder().topic("iotcore/topic").payload("message1".getBytes()).build());
        client.publish(MqttMessage.builder().topic("iotcore/topic2").payload("message2".getBytes()).build());
        client.publish(MqttMessage.builder().topic("iotcore/topic/not/in/mapping").payload("message3".getBytes()).build());

        Set<String> topicsPublished = new HashSet<>(topics);
        topicsPublished.add("iotcore/topic/not/in/mapping");
        assertThat("messages published", () -> mockMqtt5Client.getPublished().stream().map(PublishPacket::getTopic).collect(Collectors.toSet()), eventuallyEval(is(topicsPublished)));

        assertThat("handlers invoked", () -> topicsReceived, eventuallyEval(is(topics)));
    }

    @Test
    void GIVEN_client_with_subscriptions_WHEN_message_publish_fails_THEN_message_handler_not_invoked() throws Exception {
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");

        String failedPublish = "iotcore/topic";

        Set<String> topicsReceived = ConcurrentHashMap.newKeySet();
        client.updateSubscriptions(topics, m -> topicsReceived.add(m.getTopic()));

        // verify subscriptions were made
        assertThat("subscribed topics local client", () -> client.getSubscribedLocalMqttTopics(), eventuallyEval(is(topics)));
        assertThat("subscribed topics mock client", this::getMockSubscriptions, eventuallyEval(is(topics)));

        mockMqtt5Client.nextPubAckReasonCode.add(failedPublish, PubAckPacket.PubAckReasonCode.UNSPECIFIED_ERROR);

        client.publish(MqttMessage.builder().topic("iotcore/topic").payload("message1".getBytes()).build());
        client.publish(MqttMessage.builder().topic("iotcore/topic2").payload("message2".getBytes()).build());
        client.publish(MqttMessage.builder().topic("iotcore/topic/not/in/mapping").payload("message3".getBytes()).build());

        Set<String> topicsPublished = new HashSet<>(topics);
        topicsPublished.add("iotcore/topic/not/in/mapping");
        topicsPublished.remove(failedPublish);

        Set<String> expectedHandlers = new HashSet<>(topics);
        expectedHandlers.remove(failedPublish);

        assertThat("messages published", () -> mockMqtt5Client.getPublished().stream().map(PublishPacket::getTopic).collect(Collectors.toSet()), eventuallyEval(is(topicsPublished)));
        assertThat("handlers invoked", () -> topicsReceived, eventuallyEval(is(expectedHandlers)));
    }

    @Test
    void GIVEN_client_with_subscriptions_WHEN_message_publish_fails_to_execution_exception_THEN_message_handler_not_invoked(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, RuntimeException.class);

        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");

        Set<String> topicsReceived = ConcurrentHashMap.newKeySet();
        client.updateSubscriptions(topics, m -> topicsReceived.add(m.getTopic()));

        // verify subscriptions were made
        assertThat("subscribed topics local client", () -> client.getSubscribedLocalMqttTopics(), eventuallyEval(is(topics)));
        assertThat("subscribed topics mock client", this::getMockSubscriptions, eventuallyEval(is(topics)));

        mockMqtt5Client.throwExceptions = true;

        client.publish(MqttMessage.builder().topic("iotcore/topic").payload("message1".getBytes()).build());
        client.publish(MqttMessage.builder().topic("iotcore/topic2").payload("message2".getBytes()).build());
        client.publish(MqttMessage.builder().topic("iotcore/topic/not/in/mapping").payload("message3".getBytes()).build());

        Set<String> topicsPublished = new HashSet<>();
        Set<String> expectedHandlers = new HashSet<>();
        assertThat("messages published", () -> mockMqtt5Client.getPublished().stream().map(PublishPacket::getTopic).collect(Collectors.toSet()), eventuallyEval(is(topicsPublished)));
        assertThat("handlers invoked", () -> topicsReceived, eventuallyEval(is(expectedHandlers)));
    }

    @Test
    void GIVEN_client_WHEN_update_subscriptions_with_null_message_handler_THEN_throws() {
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");
        assertThrows(NullPointerException.class, () -> client.updateSubscriptions(topics, null));
    }

    @Test
    void GIVEN_client_WHEN_unsubscribe_from_topic_with_retryable_reason_code_THEN_retry_unsubscribe
            (ExtensionContext context) throws RetryableMqttOperationException {
        ignoreExceptionOfType(context, RetryableMqttOperationException.class);
        String topic = "iotcore/topic";
        String topic2 = "iotcore/topic2";
        Set<String> topics = new HashSet<>();
        topics.add(topic);
        LocalMqtt5Client clientSpy = spy(client);

        // subscribe to topics
        clientSpy.updateSubscriptions(topics, message -> {});

        topics.remove(topic);
        topics.add(topic2);
        mockMqtt5Client.nextUnsubAckReasonCode.add(topic, UnsubAckPacket.UnsubAckReasonCode.UNSPECIFIED_ERROR);

        // subscribe to new topics, this will unsubscribe from the old topics
        clientSpy.updateSubscriptions(topics, message -> {});
        assertThat("subscribed topics local client", clientSpy::getSubscribedLocalMqttTopics,
                eventuallyEval(is(topics)));
        assertThat("subscribed topics mock client", this::getMockSubscriptions, eventuallyEval(is(topics)));
        verify(clientSpy, times(2)).unsubscribe(topic);
    }

    @Test
    void GIVEN_client_with_subscription_request_WHEN_retryable_reason_code_received_THEN_subscription_will_retry
        (ExtensionContext context) throws RetryableMqttOperationException {
        ignoreExceptionOfType(context, RetryableMqttOperationException.class);
        String topic = "iotcore/topic";
        Set<String> topics = new HashSet<>();
        topics.add(topic);

        LocalMqtt5Client clientSpy = spy(client);
        mockMqtt5Client.nextSubAckReasonCode.add(topic, SubAckPacket.SubAckReasonCode.UNSPECIFIED_ERROR);
        clientSpy.updateSubscriptions(topics, message -> {});

        assertThat("subscribed topics local client", clientSpy::getSubscribedLocalMqttTopics,
                eventuallyEval(is(topics)));
        assertThat("subscribed topics mock client", this::getMockSubscriptions, eventuallyEval(is(topics)));
        verify(clientSpy, times(2)).subscribe(topic);
    }
    
    private Set<String> getMockSubscriptions() {
        return mockMqtt5Client.getSubscriptions().stream()
                .map(SubscribePacket::getSubscriptions)
                .flatMap(Collection::stream)
                .map(SubscribePacket.Subscription::getTopicFilter)
                .collect(Collectors.toSet());
    }

    private void createLocalMqtt5Client() throws MessageClientException {
        createLocalMqtt5ClientWithMqtt5Options(Collections.emptyMap());
    }

    private void createLocalMqtt5ClientWithMqtt5Options(Map<String, Mqtt5RouteOptions> opts) throws MessageClientException {
        client = new LocalMqtt5Client(
                URI.create("tcp://localhost:1883"),
                "test-client",
                BridgeConfig.DEFAULT_SESSION_EXPIRY_INTERVAL,
                BridgeConfig.DEFAULT_MAXIMUM_PACKET_SIZE,
                BridgeConfig.DEFAULT_RECEIVE_MAXIMUM,
                1L,
                1L,
                1L,
                0L,
                1L,
                1L,
                opts,
                mock(MQTTClientKeyStore.class),
                executorService,
                ses,
                null
        );

        lifecycleEvents = spy(new Mqtt5ClientOptions.LifecycleEvents() {
            @Override
            public void onAttemptingConnect(Mqtt5Client mqtt5Client, OnAttemptingConnectReturn onAttemptingConnectReturn) {
                client.getConnectionEventCallback().onAttemptingConnect(mqtt5Client, onAttemptingConnectReturn);
            }

            @Override
            public void onConnectionSuccess(Mqtt5Client mqtt5Client, OnConnectionSuccessReturn onConnectionSuccessReturn) {
                client.getConnectionEventCallback().onConnectionSuccess(mqtt5Client, onConnectionSuccessReturn);
            }

            @Override
            public void onConnectionFailure(Mqtt5Client mqtt5Client, OnConnectionFailureReturn onConnectionFailureReturn) {
                client.getConnectionEventCallback().onConnectionFailure(mqtt5Client, onConnectionFailureReturn);
            }

            @Override
            public void onDisconnection(Mqtt5Client mqtt5Client, OnDisconnectionReturn onDisconnectionReturn) {
                client.getConnectionEventCallback().onDisconnection(mqtt5Client, onDisconnectionReturn);
            }

            @Override
            public void onStopped(Mqtt5Client mqtt5Client, OnStoppedReturn onStoppedReturn) {
                client.getConnectionEventCallback().onStopped(mqtt5Client, onStoppedReturn);
            }
        });

        mockMqtt5Client = new MockMqtt5Client(
                lifecycleEvents,
                client.getPublishEventsCallback()
        );
        client.setClientSupplier(mockMqtt5Client::getClient);
        client.setClient(mockMqtt5Client.getClient());
    }
}
