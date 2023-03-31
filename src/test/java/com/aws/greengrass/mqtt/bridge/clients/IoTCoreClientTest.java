/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.clients;

import com.aws.greengrass.mqtt.bridge.model.Mqtt5RouteOptions;
import com.aws.greengrass.mqtt.bridge.model.MqttMessage;
import com.aws.greengrass.mqttclient.v5.Publish;
import com.aws.greengrass.mqttclient.v5.Subscribe;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
public class IoTCoreClientTest {

    MockMqttClient mockMqttClient = new MockMqttClient(false);
    ExecutorService executorService = TestUtils.synchronousExecutorService();
    IoTCoreClient iotCoreClient;

    @BeforeEach
    void setUp() {
        createClientWithMqtt5RouteOptions(Collections.emptyMap());
    }

    @Test
    void GIVEN_client_with_no_subscriptions_WHEN_update_subscriptions_THEN_topics_subscribed() {
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");

        iotCoreClient.updateSubscriptions(topics, message -> {});

        assertThat("subscribed topics iot core client", () -> iotCoreClient.getSubscribedIotCoreTopics(), eventuallyEval(is(topics)));
        assertThat("subscribed topics spooler client", () -> mockMqttClient.getSubscriptions().stream().map(Subscribe::getTopic).collect(Collectors.toSet()), eventuallyEval(is(topics)));
    }

    @Test
    void GIVEN_offline_client_with_no_subscriptions_WHEN_update_subscriptions_THEN_subscribe_once_online() {
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");

        setOffline();
        iotCoreClient.updateSubscriptions(topics, message -> {});

        // no topics subscribed
        assertThat("to subscribe topics", () -> iotCoreClient.getToSubscribeIotCoreTopics(), eventuallyEval(is(topics)));
        assertTrue(iotCoreClient.getSubscribedIotCoreTopics().isEmpty());
        assertTrue(mockMqttClient.getSubscriptions().isEmpty());

        setOnline();

        // verify subscriptions were made
        assertThat("subscribed topics iot core client", () -> iotCoreClient.getSubscribedIotCoreTopics(), eventuallyEval(is(topics)));
        assertThat("subscribed topics spooler client", () -> mockMqttClient.getSubscriptions().stream().map(Subscribe::getTopic).collect(Collectors.toSet()), eventuallyEval(is(topics)));
    }

    @Test
    void GIVEN_client_with_subscriptions_WHEN_stopped_THEN_topics_unsubscribed() {
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");

        iotCoreClient.updateSubscriptions(topics, message -> {});

        // verify subscriptions were made
        assertThat("subscribed topics iot core client", () -> iotCoreClient.getSubscribedIotCoreTopics(), eventuallyEval(is(topics)));
        assertThat("subscribed topics spooler client", () -> mockMqttClient.getSubscriptions().stream().map(Subscribe::getTopic).collect(Collectors.toSet()), eventuallyEval(is(topics)));

        iotCoreClient.stop();

        assertThat("iot core client unsubscribed", () -> iotCoreClient.getSubscribedIotCoreTopics().isEmpty(), eventuallyEval(is(true)));
        assertThat("spooler client unsubscribed", () -> mockMqttClient.getSubscriptions().isEmpty(), eventuallyEval(is(true)));
    }

    @Test
    void GIVEN_client_with_subscriptions_WHEN_new_topic_added_THEN_subscription_made() {
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");

        iotCoreClient.updateSubscriptions(topics, message -> {});

        // verify subscriptions were made
        assertThat("subscribed topics iot core client", () -> iotCoreClient.getSubscribedIotCoreTopics(), eventuallyEval(is(topics)));
        assertThat("subscribed topics spooler client", () -> mockMqttClient.getSubscriptions().stream().map(Subscribe::getTopic).collect(Collectors.toSet()), eventuallyEval(is(topics)));

        topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2/changed");
        topics.add("iotcore/topic3/added");

        iotCoreClient.updateSubscriptions(topics, message -> {});

        // verify subscriptions were made
        assertThat("subscribed topics iot core client", () -> iotCoreClient.getSubscribedIotCoreTopics(), eventuallyEval(is(topics)));
        assertThat("subscribed topics spooler client", () -> mockMqttClient.getSubscriptions().stream().map(Subscribe::getTopic).collect(Collectors.toSet()), eventuallyEval(is(topics)));
    }

    @Test
    void GIVEN_client_with_subscriptions_WHEN_message_published_THEN_message_handler_invoked() throws Exception {
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");

        Set<String> topicsReceived = ConcurrentHashMap.newKeySet();
        iotCoreClient.updateSubscriptions(topics, m -> topicsReceived.add(m.getTopic()));

        // verify subscriptions were made
        assertThat("subscribed topics iot core client", () -> iotCoreClient.getSubscribedIotCoreTopics(), eventuallyEval(is(topics)));
        assertThat("subscribed topics spooler client", () -> mockMqttClient.getSubscriptions().stream().map(Subscribe::getTopic).collect(Collectors.toSet()), eventuallyEval(is(topics)));

        iotCoreClient.publish(MqttMessage.builder().topic("iotcore/topic").payload("message1".getBytes()).build());
        iotCoreClient.publish(MqttMessage.builder().topic("iotcore/topic2").payload("message2".getBytes()).build());
        iotCoreClient.publish(MqttMessage.builder().topic("iotcore/topic/not/in/mapping").payload("message3".getBytes()).build());

        Set<String> topicsPublished = new HashSet<>(topics);
        topicsPublished.add("iotcore/topic/not/in/mapping");
        assertThat("messages published", () -> mockMqttClient.getPublished().stream().map(Publish::getTopic).collect(Collectors.toSet()), eventuallyEval(is(topicsPublished)));

        assertThat("handlers invoked", () -> topicsReceived, eventuallyEval(is(topics)));
    }

    @Test
    void GIVEN_client_with_subscriptions_nolocal_WHEN_message_published_THEN_message_handler_not_invoked() throws Exception {
        Map<String, Mqtt5RouteOptions> routeOptions = new HashMap<>();
        routeOptions.put("iotcore/topic", Mqtt5RouteOptions.builder().noLocal(true).build());

        createClientWithMqtt5RouteOptions(routeOptions);

        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");

        Set<String> topicsReceived = ConcurrentHashMap.newKeySet();
        iotCoreClient.updateSubscriptions(topics, m -> topicsReceived.add(m.getTopic()));

        // verify subscriptions were made
        assertThat("subscribed topics iot core client", () -> iotCoreClient.getSubscribedIotCoreTopics(), eventuallyEval(is(topics)));
        assertThat("subscribed topics spooler client", () -> mockMqttClient.getSubscriptions().stream().map(Subscribe::getTopic).collect(Collectors.toSet()), eventuallyEval(is(topics)));

        iotCoreClient.publish(MqttMessage.builder().topic("iotcore/topic").payload("message1".getBytes()).build());
        iotCoreClient.publish(MqttMessage.builder().topic("iotcore/topic2").payload("message2".getBytes()).build());

        Set<String> expectedInvokedHandlers = new HashSet<>();
        expectedInvokedHandlers.add("iotcore/topic2");
        assertThat("messages published", () -> mockMqttClient.getPublished().stream().map(Publish::getTopic).collect(Collectors.toSet()), eventuallyEval(is(topics)));
        assertThat("handlers invoked", () -> topicsReceived, eventuallyEval(is(expectedInvokedHandlers)));
    }

    @Test
    void GIVEN_iotcore_client_WHEN_update_subscriptions_with_null_message_handler_THEN_throws() {
        Set<String> topics = new HashSet<>();
        topics.add("iotcore/topic");
        topics.add("iotcore/topic2");
        assertThrows(NullPointerException.class, () -> iotCoreClient.updateSubscriptions(topics, null));
    }

    private void setOnline() {
        mockMqttClient.online();
        iotCoreClient.getConnectionCallbacks().onConnectionResumed(mockMqttClient.isCleanSession());
    }

    private void setOffline() {
        mockMqttClient.offline();
        iotCoreClient.getConnectionCallbacks().onConnectionInterrupted(0);
    }

    private void createClientWithMqtt5RouteOptions(Map<String, Mqtt5RouteOptions> opts) {
        iotCoreClient = new IoTCoreClient(
                mockMqttClient.getMqttClient(),
                executorService,
                opts
        );
    }
}
