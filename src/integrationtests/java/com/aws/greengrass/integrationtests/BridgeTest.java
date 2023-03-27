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

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@BridgeIntegrationTest
public class BridgeTest {

    private static final long AWAIT_TIMEOUT_SECONDS = 30;

    BridgeIntegrationTestContext context;

    @TestWithAllBrokers
    @WithKernel("mqtt3_local_and_iotcore.yaml")
    void GIVEN_mqtt3_and_mapping_between_local_and_iotcore_WHEN_iotcore_message_received_THEN_message_bridged_to_local(Broker broker) throws Exception {
        CountDownLatch messageReceived = new CountDownLatch(1);
        AtomicReference<MqttMessage> m = new AtomicReference<>();
        context.getLocalV3Client().getMqttClientInternal().subscribe("topic/toLocal", (topic, message) -> {
            messageReceived.countDown();
            m.compareAndSet(null, MqttMessage.fromPahoMQTT3(topic, message));
        });

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

        assertTrue(messageReceived.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));

        MqttMessage expectedMessage = MqttMessage.builder()
                .topic("topic/toLocal")
                .payload("message".getBytes(StandardCharsets.UTF_8))
                .build();

        assertEquals(expectedMessage, m.get());
    }

    @TestWithAllBrokers
    @WithKernel("mqtt3_local_and_iotcore.yaml")
    void GIVEN_mqtt3_and_mapping_between_local_and_iotcore_WHEN_local_message_received_THEN_message_bridged_to_iotcore(Broker broker) throws Exception {
        CountDownLatch messageReceived = new CountDownLatch(1);
        AtomicReference<MqttMessage> m = new AtomicReference<>();
        context.getIotCoreClient().getIotMqttClient().subscribe(Subscribe.builder()
                .topic("topic/toIotCore")
                .callback(p -> {
                    messageReceived.countDown();
                    m.compareAndSet(null, MqttMessage.fromSpoolerV5Model(p));
                })
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

        assertTrue(messageReceived.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));

        MqttMessage expectedMessage = MqttMessage.builder()
                .topic("topic/toIotCore")
                .payload("message".getBytes(StandardCharsets.UTF_8))
                .build();

        assertEquals(expectedMessage, m.get());
    }

    @TestWithMqtt5Broker
    @WithKernel("mqtt5_local_and_iotcore.yaml")
    void GIVEN_mqtt5_and_mapping_between_local_and_iotcore_WHEN_local_message_received_THEN_message_bridged_to_iotcore(Broker broker) throws Exception {
        CountDownLatch messageReceived = new CountDownLatch(1);
        AtomicReference<MqttMessage> m = new AtomicReference<>();
        context.getIotCoreClient().getIotMqttClient().subscribe(Subscribe.builder()
                .topic("topic/toIotCore")
                .callback(p -> {
                    m.compareAndSet(null, MqttMessage.fromSpoolerV5Model(p));
                    messageReceived.countDown();
                })
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

        assertTrue(messageReceived.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
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

        assertEquals(expectedMessage, m.get());
    }
}
