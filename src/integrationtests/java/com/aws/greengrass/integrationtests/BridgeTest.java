/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.integrationtests.extensions.BridgeIntegrationTest;
import com.aws.greengrass.integrationtests.extensions.BridgeIntegrationTestContext;
import com.aws.greengrass.integrationtests.extensions.Broker;
import com.aws.greengrass.integrationtests.extensions.TestWithAllBrokers;
import com.aws.greengrass.integrationtests.extensions.TestWithMqtt3Broker;
import com.aws.greengrass.integrationtests.extensions.TestWithMqtt5Broker;
import com.aws.greengrass.integrationtests.extensions.WithKernel;
import com.aws.greengrass.mqtt.bridge.model.MqttMessage;
import com.aws.greengrass.mqttclient.v5.Publish;
import com.aws.greengrass.mqttclient.v5.Subscribe;
import com.aws.greengrass.mqttclient.v5.UserProperty;
import com.aws.greengrass.util.Pair;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.aws.greengrass.testcommons.testutilities.TestUtils.asyncAssertOnBiConsumer;
import static com.aws.greengrass.testcommons.testutilities.TestUtils.asyncAssertOnConsumer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@BridgeIntegrationTest
public class BridgeTest {

    /**
     * Operations are between local broker and a mocked IoT Core,
     * so we expect them to happen quickly.
     */
    private static final long AWAIT_TIMEOUT_SECONDS = 2;

    BridgeIntegrationTestContext context;

    @TestWithMqtt5Broker
    @WithKernel("mqtt5_local_to_iotcore_nolocal.yaml")
    void GIVEN_mqtt5_and_mapping_between_local_and_iotcore_with_nolocal_WHEN_message_published_THEN_message_does_not_loop(Broker broker) throws Exception {
        Pair<CompletableFuture<Void>, Consumer<Publish>> iotCoreTopicSubscription
                = asyncAssertOnConsumer(p -> {}, 0);

        context.getIotCoreClient().getIotMqttClient().subscribe(Subscribe.builder()
                .topic("topic/toIotCore")
                .callback(iotCoreTopicSubscription.getRight())
                .build()).get(5L, TimeUnit.SECONDS);

        context.getLocalV5Client().publish(
                MqttMessage.builder()
                        .topic("topic/toIotCore")
                        .payload("message".getBytes(StandardCharsets.UTF_8))
                        // mqtt5-specific fields below.
                        .userProperties(Collections.singletonList(new UserProperty("key", "val")))
                        .responseTopic("response topic")
                        .messageExpiryIntervalSeconds(1234L)
                        .payloadFormat(Publish.PayloadFormatIndicator.UTF8)
                        .contentType("contentType")
                        .build());

        assertThrows(TimeoutException.class, () -> iotCoreTopicSubscription.getLeft().get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }

    @TestWithMqtt3Broker
    @WithKernel("mqtt3_local_and_iotcore.yaml")
    void GIVEN_mqtt3_and_mapping_between_local_and_iotcore_WHEN_iotcore_message_received_THEN_message_bridged_to_local(Broker broker) throws Exception {
        MqttMessage expectedMessage = MqttMessage.builder()
                .topic("topic/toLocal")
                .payload("message".getBytes(StandardCharsets.UTF_8))
                .build();

        Pair<CompletableFuture<Void>, BiConsumer<String, org.eclipse.paho.client.mqttv3.MqttMessage>> subscribeCallback
                = asyncAssertOnBiConsumer((topic, message) -> assertEquals(expectedMessage, MqttMessage.fromPahoMQTT3(topic, message)));

        context.getLocalV3Client().getMqttClientInternal().subscribe("topic/toLocal",
                (topic, message) -> subscribeCallback.getRight().accept(topic, message));

        context.getIotCoreClient().publish(
                MqttMessage.builder()
                        .topic("topic/toLocal")
                        .payload("message".getBytes(StandardCharsets.UTF_8))
                        // mqtt5-specific fields below.
                        // this test uses mqtt3 protocol,
                        // so we expect this fields to be dropped during bridging
                        .userProperties(Collections.singletonList(new UserProperty("key", "val")))
                        .responseTopic("response topic")
                        .messageExpiryIntervalSeconds(1234L)
                        .payloadFormat(Publish.PayloadFormatIndicator.UTF8)
                        .contentType("contentType")
                        .build());

        subscribeCallback.getLeft().get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @TestWithMqtt3Broker
    @WithKernel("mqtt3_local_and_iotcore.yaml")
    void GIVEN_mqtt3_and_mapping_between_local_and_iotcore_WHEN_local_message_received_THEN_message_bridged_to_iotcore(Broker broker) throws Exception {
        MqttMessage expectedMessage = MqttMessage.builder()
                .topic("topic/toIotCore")
                .payload("message".getBytes(StandardCharsets.UTF_8))
                .build();

        Pair<CompletableFuture<Void>, Consumer<Publish>> subscribeCallback
                = asyncAssertOnConsumer(p -> assertEquals(expectedMessage, MqttMessage.fromSpoolerV5Model(p)));

        context.getIotCoreClient().getIotMqttClient().subscribe(Subscribe.builder()
                .topic("topic/toIotCore")
                .callback(subscribeCallback.getRight())
                .build());

        context.getLocalV3Client().publish(
                MqttMessage.builder()
                        .topic("topic/toIotCore")
                        .payload("message".getBytes(StandardCharsets.UTF_8))
                        // mqtt5-specific fields below.
                        // will be ignored by v3 mqtt client
                        .userProperties(Collections.singletonList(new UserProperty("key", "val")))
                        .responseTopic("response topic")
                        .messageExpiryIntervalSeconds(1234L)
                        .payloadFormat(Publish.PayloadFormatIndicator.UTF8)
                        .contentType("contentType")
                        .build());

        subscribeCallback.getLeft().get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @TestWithMqtt5Broker
    @WithKernel("mqtt5_local_and_iotcore.yaml")
    void GIVEN_mqtt5_and_mapping_between_local_and_iotcore_WHEN_local_message_received_THEN_message_bridged_to_iotcore(Broker broker) throws Exception {
        MqttMessage expectedMessage = MqttMessage.builder()
                .topic("topic/toIotCore")
                .payload("message".getBytes(StandardCharsets.UTF_8))
                // mqtt5-specific fields below.
                .userProperties(Collections.singletonList(new UserProperty("key", "val")))
                .responseTopic("response topic")
                .messageExpiryIntervalSeconds(1234L)
                .payloadFormat(Publish.PayloadFormatIndicator.UTF8)
                .contentType("contentType")
                .build();

        Pair<CompletableFuture<Void>, Consumer<Publish>> subscribeCallback
                = asyncAssertOnConsumer(p -> assertEquals(expectedMessage, MqttMessage.fromSpoolerV5Model(p)));

        context.getIotCoreClient().getIotMqttClient().subscribe(Subscribe.builder()
                .topic("topic/toIotCore")
                .callback(subscribeCallback.getRight())
                .build());

        context.getLocalV5Client().publish(
                MqttMessage.builder()
                        .topic("topic/toIotCore")
                        .payload("message".getBytes(StandardCharsets.UTF_8))
                        // mqtt5-specific fields below.
                        .userProperties(Collections.singletonList(new UserProperty("key", "val")))
                        .responseTopic("response topic")
                        .messageExpiryIntervalSeconds(1234L)
                        .payloadFormat(Publish.PayloadFormatIndicator.UTF8)
                        .contentType("contentType")
                        .build());

        subscribeCallback.getLeft().get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @TestWithMqtt5Broker
    @WithKernel("mqtt5_local_to_iotcore_retain_as_published.yaml")
    void GIVEN_mqtt5_and_local_mapping_with_retainAsPublished_WHEN_message_published_with_retain_THEN_message_bridged_with_retain_flag(Broker broker)
            throws Exception {
        String topic = "topic/toIotCore";
        Set<String> topics = new HashSet<>();
        topics.add(topic);

        MqttMessage expectedMessage = MqttMessage.builder()
                .topic(topic)
                .payload("message".getBytes(StandardCharsets.UTF_8))
                // mqtt5-specific fields below.
                .userProperties(Collections.singletonList(new UserProperty("key", "val")))
                .responseTopic("response topic")
                .messageExpiryIntervalSeconds(1234L)
                .payloadFormat(Publish.PayloadFormatIndicator.UTF8)
                .contentType("contentType")
                .retain(true)
                .build();

        Pair<CompletableFuture<Void>, Consumer<Publish>> publishHandler = asyncAssertOnConsumer(p -> {
           MqttMessage msg = MqttMessage.fromSpoolerV5Model(p);
           assertEquals(Arrays.toString(expectedMessage.getPayload()), Arrays.toString(msg.getPayload()));
           assertEquals(expectedMessage.isRetain(), msg.isRetain());
        });

        context.getIotCoreClient().getIotMqttClient().subscribe(Subscribe.builder()
                .topic(topic)
                .callback(publishHandler.getRight())
                .build());

        context.getLocalV5Client().publish(
                MqttMessage.builder()
                        .topic(topic)
                        .payload("message".getBytes(StandardCharsets.UTF_8))
                        // mqtt5-specific fields below.
                        .userProperties(Collections.singletonList(new UserProperty("key", "val")))
                        .responseTopic("response topic")
                        .messageExpiryIntervalSeconds(1234L)
                        .payloadFormat(Publish.PayloadFormatIndicator.UTF8)
                        .contentType("contentType")
                        .retain(true)
                        .build());

        publishHandler.getLeft().get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        Publish msg = context.getMockMqttClient().getPublished().get(0);
        assertEquals(expectedMessage.isRetain(), msg.isRetain());
    }

    @TestWithMqtt5Broker
    @WithKernel("mqtt5_local_to_iotcore_retain_as_published.yaml")
    void GIVEN_mqtt5_and_local_mapping_with_retainAsPublished_WHEN_message_published_without_retain_THEN_message_bridged_without_retain_flag(Broker broker)
            throws Exception {
        String topic = "topic/toIotCore";
        Set<String> topics = new HashSet<>();
        topics.add(topic);

        MqttMessage expectedMessage = MqttMessage.builder()
                .topic(topic)
                .payload("message".getBytes(StandardCharsets.UTF_8))
                // mqtt5-specific fields below.
                .userProperties(Collections.singletonList(new UserProperty("key", "val")))
                .responseTopic("response topic")
                .messageExpiryIntervalSeconds(1234L)
                .payloadFormat(Publish.PayloadFormatIndicator.UTF8)
                .contentType("contentType")
                .retain(false)
                .build();

        Pair<CompletableFuture<Void>, Consumer<Publish>> publishHandler = asyncAssertOnConsumer(p -> {
            MqttMessage msg = MqttMessage.fromSpoolerV5Model(p);
            assertEquals(Arrays.toString(expectedMessage.getPayload()), Arrays.toString(msg.getPayload()));
            assertEquals(expectedMessage.isRetain(), msg.isRetain());
        });

        context.getIotCoreClient().getIotMqttClient().subscribe(Subscribe.builder()
                .topic(topic)
                .callback(publishHandler.getRight())
                .build());

        context.getLocalV5Client().publish(
                MqttMessage.builder()
                        .topic(topic)
                        .payload("message".getBytes(StandardCharsets.UTF_8))
                        // mqtt5-specific fields below.
                        .userProperties(Collections.singletonList(new UserProperty("key", "val")))
                        .responseTopic("response topic")
                        .messageExpiryIntervalSeconds(1234L)
                        .payloadFormat(Publish.PayloadFormatIndicator.UTF8)
                        .contentType("contentType")
                        .build());

        publishHandler.getLeft().get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        Publish msg = context.getMockMqttClient().getPublished().get(0);
        assertEquals(expectedMessage.isRetain(), msg.isRetain());
    }

    @TestWithMqtt5Broker
    @WithKernel("mqtt5_local_and_iotcore.yaml")
    void GIVEN_mqtt5_and_local_mapping_without_retainAsPublished_WHEN_message_published_with_retain_THEN_message_bridged_without_retain_flag(Broker broker)
            throws Exception {
        String topic = "topic/toIotCore";
        Set<String> topics = new HashSet<>();
        topics.add(topic);

        MqttMessage expectedMessage = MqttMessage.builder()
                .topic(topic)
                .payload("message".getBytes(StandardCharsets.UTF_8))
                // mqtt5-specific fields below.
                .userProperties(Collections.singletonList(new UserProperty("key", "val")))
                .responseTopic("response topic")
                .messageExpiryIntervalSeconds(1234L)
                .payloadFormat(Publish.PayloadFormatIndicator.UTF8)
                .contentType("contentType")
                .retain(false)
                .build();

        Pair<CompletableFuture<Void>, Consumer<Publish>> publishHandler = asyncAssertOnConsumer(p -> {
            MqttMessage msg = MqttMessage.fromSpoolerV5Model(p);
            assertEquals(Arrays.toString(expectedMessage.getPayload()), Arrays.toString(msg.getPayload()));
            assertEquals(expectedMessage.isRetain(), msg.isRetain());
        });

        context.getIotCoreClient().getIotMqttClient().subscribe(Subscribe.builder()
                .topic(topic)
                .callback(publishHandler.getRight())
                .build());

        context.getLocalV5Client().publish(
                MqttMessage.builder()
                        .topic(topic)
                        .payload("message".getBytes(StandardCharsets.UTF_8))
                        // mqtt5-specific fields below.
                        .userProperties(Collections.singletonList(new UserProperty("key", "val")))
                        .responseTopic("response topic")
                        .messageExpiryIntervalSeconds(1234L)
                        .payloadFormat(Publish.PayloadFormatIndicator.UTF8)
                        .contentType("contentType")
                        .retain(true)
                        .build());

        publishHandler.getLeft().get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        Publish msg = context.getMockMqttClient().getPublished().get(0);
        assertEquals(expectedMessage.isRetain(), msg.isRetain());
    }
}
