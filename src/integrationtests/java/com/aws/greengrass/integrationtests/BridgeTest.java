/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.integrationtests.extensions.BridgeIntegrationTest;
import com.aws.greengrass.integrationtests.extensions.BridgeIntegrationTestContext;
import com.aws.greengrass.integrationtests.extensions.Broker;
import com.aws.greengrass.integrationtests.extensions.TestWithAllBrokers;
import com.aws.greengrass.integrationtests.extensions.TestWithMqtt5Broker;
import com.aws.greengrass.integrationtests.extensions.WithKernel;
import com.aws.greengrass.mqtt.bridge.model.MqttMessage;
import com.aws.greengrass.mqttclient.v5.Publish;
import com.aws.greengrass.mqttclient.v5.Subscribe;
import com.aws.greengrass.mqttclient.v5.UserProperty;
import com.aws.greengrass.util.Pair;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.aws.greengrass.testcommons.testutilities.TestUtils.asyncAssertOnBiConsumer;
import static com.aws.greengrass.testcommons.testutilities.TestUtils.asyncAssertOnConsumer;
import static org.junit.jupiter.api.Assertions.assertEquals;

@BridgeIntegrationTest
public class BridgeTest {

    /**
     * Operations are between local broker and a mocked IoT Core,
     * so we expect them to happen quickly.
     */
    private static final long AWAIT_TIMEOUT_SECONDS = 2;

    BridgeIntegrationTestContext context;

    @TestWithAllBrokers
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

    @TestWithAllBrokers
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
}
