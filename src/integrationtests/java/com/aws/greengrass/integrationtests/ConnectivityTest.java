/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.integrationtests.extensions.BridgeIntegrationTest;
import com.aws.greengrass.integrationtests.extensions.BridgeIntegrationTestContext;
import com.aws.greengrass.integrationtests.extensions.Broker;
import com.aws.greengrass.integrationtests.extensions.TestWithMqtt5Broker;
import com.aws.greengrass.integrationtests.extensions.WithKernel;
import com.aws.greengrass.mqtt.bridge.TopicMapping;
import com.aws.greengrass.mqtt.bridge.clients.MessageClientException;
import com.aws.greengrass.mqtt.bridge.model.MqttMessage;
import com.aws.greengrass.util.Pair;
import com.aws.greengrass.util.Utils;
import org.junit.jupiter.api.function.Executable;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.aws.greengrass.testcommons.testutilities.TestUtils.asyncAssertOnConsumer;
import static com.github.grantwest.eventually.EventuallyLambdaMatcher.eventuallyEval;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

// TODO support MQTT5 local client
@BridgeIntegrationTest
public class ConnectivityTest {

    /**
     * Operations are between local broker and a mocked IoT Core,
     * so we expect them to happen quickly.
     */
    private static final long AWAIT_TIMEOUT_SECONDS = 2;

    private static final String TOPIC_TO_LOCAL = "topic/toLocal";
    private static final String TOPIC_OTHER = "topic/other";

    private static final Map<String, TopicMapping.MappingEntry> TO_LOCAL_MAPPING = Utils.immutableMap(
            "toLocal", TopicMapping.MappingEntry.builder()
                    .topic(TOPIC_TO_LOCAL)
                    .source(TopicMapping.TopicType.LocalMqtt)
                    .target(TopicMapping.TopicType.LocalMqtt)
                    .targetTopicPrefix("ack/")
                    .build());
    private static final Map<String, TopicMapping.MappingEntry> OTHER_MAPPING = Utils.immutableMap(
            "other", TopicMapping.MappingEntry.builder()
                    .topic(TOPIC_OTHER)
                    .source(TopicMapping.TopicType.LocalMqtt)
                    .target(TopicMapping.TopicType.LocalMqtt)
                    .build());

    BridgeIntegrationTestContext context;
    Consumer<MqttMessage> originalMessageHandler;

    @TestWithMqtt5Broker
    @WithKernel("mqtt3_local_to_local.yaml")
    void GIVEN_local_mqtt3_client_WHEN_broker_connectivity_is_lost_THEN_client_will_eventually_update_subscriptions(Broker broker) throws Throwable {
        originalMessageHandler = context.getLocalV3Client().getMessageHandler();

        // verify we can bridge a message locally
        publishToTopicsAndVerify(Utils.immutableMap(TOPIC_TO_LOCAL, "message"));

        // remove the subscription topic
        updateTopicMapping(Collections.emptyMap());
        assertThat("client is subscribed to topics",
                () -> context.getLocalV3Client().getSubscribedLocalMqttTopics(), eventuallyEval(empty()));

        // verify we can't publish message to topic anymore
        publishToTopicsAndVerifyFailure(Utils.immutableMap(TOPIC_TO_LOCAL, "message"));

        // re-add the subscription topic
        updateTopicMapping(TO_LOCAL_MAPPING);
        assertThat("client is subscribed to topics",
                () -> context.getLocalV3Client().getSubscribedLocalMqttTopics(), eventuallyEval(is(Collections.singleton(TOPIC_TO_LOCAL))));

        // publish message to the original topic again
        publishToTopicsAndVerify(Utils.immutableMap(TOPIC_TO_LOCAL, "message"));

        // go offline
        context.stopBroker();
        assertThat("client is disconnected",
                () -> context.getLocalV3Client().getMqttClientInternal().isConnected(), eventuallyEval(is(false)));

        // remove the subscription topic
        updateTopicMapping(Collections.emptyMap());
        assertThat("client is unsubscribed",
                () -> context.getLocalV3Client().getSubscribedLocalMqttTopics(), eventuallyEval(empty()));

        // go online
        context.startBroker();
        assertThat("client is connected",
                () -> context.getLocalV3Client().getMqttClientInternal().isConnected(), eventuallyEval(is(true)));

        // publish message fails
        publishToTopicsAndVerifyFailure(Utils.immutableMap(TOPIC_TO_LOCAL, "message"));

        // go offline
        context.stopBroker();
        assertThat("client is disconnected",
                () -> context.getLocalV3Client().getMqttClientInternal().isConnected(), eventuallyEval(is(false)));

        // re-add the subscription and a new one
        updateTopicMapping(merge(TO_LOCAL_MAPPING, OTHER_MAPPING));

        // go online
        context.startBroker();

        // verify we can publish to both topics
        assertThat("client is subscribed to topics",
                () -> context.getLocalV3Client().getSubscribedLocalMqttTopics(), eventuallyEval(is(Arrays.asList(TOPIC_TO_LOCAL, TOPIC_OTHER).stream().collect(Collectors.toSet()))));
        publishToTopicsAndVerify(Utils.immutableMap(
                TOPIC_TO_LOCAL, "message",
                TOPIC_OTHER, "other_message"));
    }

    private void publishToTopicsAndVerify(Map<String, String> messagesByTopic) throws Throwable {
        publishToTopicsAndVerify(messagesByTopic, true);
    }

    private void publishToTopicsAndVerifyFailure(Map<String, String> messagesByTopic) throws Throwable {
        publishToTopicsAndVerify(messagesByTopic, false);
    }

    private void publishToTopicsAndVerify(Map<String, String> messagesByTopic, boolean publishExpected) throws Throwable {
        Pair<CompletableFuture<Void>, Consumer<MqttMessage>> cb = asyncAssertOnConsumer(originalMessageHandler, messagesByTopic.size());
        context.getLocalV3Client().setMessageHandler(cb.getRight());

        messagesByTopic.forEach((topic, message) -> {
            try {
                context.getLocalV3Client().publish(
                        MqttMessage.builder()
                                .topic(topic)
                                .payload(message.getBytes(StandardCharsets.UTF_8))
                                .build());
            } catch (MessageClientException e) {
                fail(e);
            }
        });
        Executable awaitCallback = () -> cb.getLeft().get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (publishExpected) {
            awaitCallback.execute();
        } else {
            assertThrows(TimeoutException.class, awaitCallback);
        }
    }

    private void updateTopicMapping(Map<String, TopicMapping.MappingEntry> mapping) {
        context.getFromContext(TopicMapping.class).updateMapping(mapping);
    }

    private static <K,V> Map<K,V> merge(Map<K,V> mapA, Map<K,V> mapB) {
        return Stream.of(mapA, mapB)
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue));
    }
}
