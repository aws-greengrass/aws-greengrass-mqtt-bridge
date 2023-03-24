/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.clients;

import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.crt.mqtt5.packets.SubscribePacket;

import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static com.github.grantwest.eventually.EventuallyLambdaMatcher.eventuallyEval;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

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
        assertThat("subscribed topics mock client", () -> mockMqtt5Client.getSubscriptions().stream().map(SubscribePacket::getSubscriptions).flatMap(Collection::stream).map(SubscribePacket.Subscription::getTopicFilter).collect(Collectors.toSet()), eventuallyEval(is(topics)));
    }
}
