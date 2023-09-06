/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.integrationtests.extensions.BridgeIntegrationTestContext;
import com.aws.greengrass.integrationtests.extensions.BridgePerformanceTest;
import com.aws.greengrass.integrationtests.extensions.Broker;
import com.aws.greengrass.integrationtests.extensions.TestWithMqtt5Broker;
import com.aws.greengrass.integrationtests.extensions.WithKernel;
import com.aws.greengrass.mqtt.bridge.clients.MessageClient;
import com.aws.greengrass.mqtt.bridge.clients.MessageClientException;
import com.aws.greengrass.mqtt.bridge.model.MqttMessage;
import com.aws.greengrass.util.Pair;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static com.aws.greengrass.testcommons.testutilities.TestUtils.asyncAssertOnConsumer;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@BridgePerformanceTest
public class LocalBridgingPerformanceTest {

    private static final Duration PERF_TEST_DURATION = Duration.ofSeconds(20);
    private static final int TPS_MEDIUM = 500;
    private static final int TPS_HIGH = 1000;
    private static final String PAYLOAD = "hello world";
    private static final String PUBLISH_TOPIC = "topic/toLocal";

    ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);
    BridgeIntegrationTestContext context;

    @AfterEach
    void tearDown() {
        ses.shutdownNow();
    }

    @TestWithMqtt5Broker
    @WithKernel("mqtt5_local_to_local.yaml")
    void GIVEN_mqtt5_local_to_local_bridge_WHEN_messages_sent_at_tps_THEN_messages_bridged(Broker broker) throws Exception {
        LocalBridgingPerfTest.builder()
                .tps(TPS_MEDIUM)
                .duration(PERF_TEST_DURATION)
                .payload(PAYLOAD)
                .publishTopic(PUBLISH_TOPIC)
                .client(context.getLocalMqttClient())
                .ses(ses)
                .build()
                .run();
    }

    @TestWithMqtt5Broker
    @WithKernel("mqtt5_local_to_local.yaml")
    void GIVEN_mqtt5_local_to_local_bridge_WHEN_messages_sent_at_high_tps_THEN_messages_bridged(Broker broker) throws Exception {
        LocalBridgingPerfTest.builder()
                .tps(TPS_HIGH)
                .duration(PERF_TEST_DURATION)
                .payload(PAYLOAD)
                .publishTopic(PUBLISH_TOPIC)
                .client(context.getLocalMqttClient())
                .ses(ses)
                .build()
                .run();
    }

    @TestWithMqtt5Broker
    @WithKernel("mqtt3_local_to_local.yaml")
    void GIVEN_mqtt3_local_to_local_bridge_WHEN_messages_sent_at_tps_THEN_messages_bridged(Broker broker, ExtensionContext extensionContext) throws Exception {
        ignoreExceptionOfType(extensionContext, InterruptedException.class);
        LocalBridgingPerfTest.builder()
                .tps(TPS_MEDIUM)
                .duration(PERF_TEST_DURATION)
                .payload(PAYLOAD)
                .publishTopic(PUBLISH_TOPIC)
                .client(context.getLocalMqttClient())
                .ses(ses)
                .build()
                .run();
    }

    @TestWithMqtt5Broker
    @WithKernel("mqtt3_local_to_local.yaml")
    void GIVEN_mqtt3_local_to_local_bridge_WHEN_messages_sent_at_high_tps_THEN_messages_bridged(Broker broker, ExtensionContext extensionContext) throws Exception {
        ignoreExceptionOfType(extensionContext, InterruptedException.class);
        LocalBridgingPerfTest.builder()
                .tps(TPS_HIGH)
                .duration(PERF_TEST_DURATION)
                .payload(PAYLOAD)
                .publishTopic(PUBLISH_TOPIC)
                .client(context.getLocalMqttClient())
                .ses(ses)
                .build()
                .run();
    }

    @Builder
    @RequiredArgsConstructor
    static class LocalBridgingPerfTest implements Runnable {

        private final int tps;
        private final Duration duration;

        private final String publishTopic;
        private final String payload;

        private final MessageClient<MqttMessage> client;
        private final ScheduledExecutorService ses;

        @Override
        public void run() {
            try {
                AtomicInteger actualMessagesBridged = new AtomicInteger();

                Consumer<MqttMessage> handler = client.getMessageHandler();
                Pair<CompletableFuture<Void>, Consumer<MqttMessage>> cb
                        = asyncAssertOnConsumer(message -> {
                    actualMessagesBridged.incrementAndGet();
                    handler.accept(message);
                });
                client.updateSubscriptions(Collections.singleton(publishTopic), cb.getRight());

                CountDownLatch testFinished = new CountDownLatch(1);
                ses.schedule(testFinished::countDown, duration.toMillis(), TimeUnit.MILLISECONDS);
                byte[] payloadEncoded = payload.getBytes(StandardCharsets.UTF_8);
                ses.scheduleAtFixedRate(() -> {
                    try {
                        client.publish(
                                MqttMessage.builder()
                                        .topic(publishTopic)
                                        .payload(payloadEncoded)
                                        .build());
                    } catch (MessageClientException ignore) {
                    }
                }, 0L, (long) (TimeUnit.SECONDS.toMicros(1) / (double) tps), TimeUnit.MICROSECONDS);

                testFinished.await();

                ses.shutdown();
                assertTrue(ses.awaitTermination(5L, TimeUnit.SECONDS));

                long expectedMessages = duration.getSeconds() * tps;
                int actualMessages = actualMessagesBridged.get();
                double expectedReceiveRate = 0.99;
                assertTrue(((double) actualMessages / expectedMessages) >= expectedReceiveRate,
                        String.format("Did not receive enough messages (%d/%d) < %f%%", actualMessages, expectedMessages, expectedReceiveRate * 100));
            } catch (InterruptedException e) {
                fail(e);
            }
        }
    }
}
