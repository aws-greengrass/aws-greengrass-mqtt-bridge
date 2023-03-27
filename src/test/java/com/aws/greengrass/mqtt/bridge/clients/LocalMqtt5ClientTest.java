/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.clients;

import com.aws.greengrass.mqtt.bridge.model.MqttMessage;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.crt.mqtt5.packets.PublishPacket;
import software.amazon.awssdk.crt.mqtt5.packets.SubscribePacket;

import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static com.github.grantwest.eventually.EventuallyLambdaMatcher.eventuallyEval;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class LocalMqtt5ClientTest {

    ExecutorService executorService = TestUtils.synchronousExecutorService();

    MockMqtt5Client mockMqtt5Client;

    LocalMqtt5Client client;

    @BeforeEach
    void setUp() {
        client = new LocalMqtt5Client(
                URI.create("tcp://localhost:1883"),
                "test-client",
                executorService,
                null
        );
        mockMqtt5Client = new MockMqtt5Client(
                client.getConnectionEventCallback(),
                client.getPublishEventsCallback()
        );
        client.setClient(mockMqtt5Client.getClient());
        client.start();
    }

    @AfterEach
    void tearDown() {
        client.stop();
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
    void GIVEN_offline_client_with_no_subscriptions_WHEN_update_subscriptions_THEN_subscribe_once_online() {
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");

        mockMqtt5Client.offline();
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
    void GIVEN_client_WHEN_update_subscriptions_with_null_message_handler_THEN_throws() {
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");
        assertThrows(NullPointerException.class, () -> client.updateSubscriptions(topics, null));
    }
    
    private Set<String> getMockSubscriptions() {
        return mockMqtt5Client.getSubscriptions().stream()
                .map(SubscribePacket::getSubscriptions)
                .flatMap(Collection::stream)
                .map(SubscribePacket.Subscription::getTopicFilter)
                .collect(Collectors.toSet());
    }
}
