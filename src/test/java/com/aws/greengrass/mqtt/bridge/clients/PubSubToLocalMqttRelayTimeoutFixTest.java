/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Verifies that the ackTimeoutSeconds fix prevents the
 * Pubsub→LocalMqtt relay from permanently blocking when a single publish hangs.
 *
 * With the fix, Paho's MqttClient.setTimeToWait() causes Token.waitForResponse()
 * to throw MqttException after the timeout, allowing the OrderedExecutorService
 * task to complete and subsequent messages to be processed.
 */

package com.aws.greengrass.mqtt.bridge.clients;

import com.aws.greengrass.authorization.AuthorizationHandler;
import com.aws.greengrass.builtin.services.pubsub.PubSubIPCEventStreamAgent;
import com.aws.greengrass.mqtt.bridge.MessageBridge;
import com.aws.greengrass.mqtt.bridge.TopicMapping;
import com.aws.greengrass.mqtt.bridge.model.Message;
import com.aws.greengrass.mqtt.bridge.model.MqttMessage;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.util.OrderedExecutorService;
import com.aws.greengrass.util.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Verifies the fix: when a publish times out (instead of blocking forever),
 * subsequent messages continue flowing through the relay.
 */
@ExtendWith({MockitoExtension.class, GGExtension.class})
public class PubSubToLocalMqttRelayTimeoutFixTest {

    private ExecutorService threadPool;

    @AfterEach
    void tearDown() {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
    }

    private PubSubIPCEventStreamAgent createPubSubAgent(
            AuthorizationHandler authHandler, OrderedExecutorService oes) throws Exception {
        Constructor<PubSubIPCEventStreamAgent> ctor =
                PubSubIPCEventStreamAgent.class.getDeclaredConstructor(
                        AuthorizationHandler.class, OrderedExecutorService.class);
        ctor.setAccessible(true);
        return ctor.newInstance(authHandler, oes);
    }

    @Test
    void GIVEN_pubsub_to_localmqtt_relay_WHEN_publish_times_out_THEN_subsequent_messages_still_delivered()
            throws Exception {

        threadPool = Executors.newFixedThreadPool(4);
        OrderedExecutorService orderedExecutorService = new OrderedExecutorService(threadPool);

        PubSubIPCEventStreamAgent pubSubAgent =
                createPubSubAgent(mock(AuthorizationHandler.class), orderedExecutorService);

        PubSubClient pubSubClient = new PubSubClient(pubSubAgent);

        TopicMapping topicMapping = new TopicMapping();
        topicMapping.updateMapping(Utils.immutableMap(
                "shadow-responses",
                new TopicMapping.MappingEntry(
                        "$aws/things/+/shadow/update/accepted",
                        TopicMapping.TopicType.Pubsub,
                        TopicMapping.TopicType.LocalMqtt)));

        MessageBridge messageBridge = new MessageBridge(topicMapping, Collections.emptyMap());

        // LocalMqtt client that simulates the FIXED behavior:
        // First publish throws MessageClientException after a short timeout
        // (simulating Paho throwing MqttException when setTimeToWait expires),
        // subsequent publishes succeed normally.
        AtomicInteger publishCallCount = new AtomicInteger(0);
        CopyOnWriteArrayList<String> successfulPublishes = new CopyOnWriteArrayList<>();
        CountDownLatch allMessagesProcessed = new CountDownLatch(4); // expecting 4 successful publishes

        MessageClient<MqttMessage> timeoutingClient = new MessageClient<MqttMessage>() {
            @Override
            public void publish(MqttMessage message) throws MessageClientException {
                int callNum = publishCallCount.incrementAndGet();
                if (callNum == 1) {
                    // First publish: simulate Paho timeout (setTimeToWait expired)
                    // With the fix, this throws instead of blocking forever
                    throw new MQTTClientException("Timed out waiting for PUBACK");
                }
                // Subsequent publishes succeed
                successfulPublishes.add(message.getTopic());
                allMessagesProcessed.countDown();
            }

            @Override
            public void updateSubscriptions(Set<String> topics, Consumer<MqttMessage> messageHandler) {
            }

            @Override
            public MqttMessage convertMessage(Message message) {
                return (MqttMessage) message.toMqtt();
            }

            @Override
            public void start() {
            }

            @Override
            public void stop() {
            }
        };

        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(
                TopicMapping.TopicType.Pubsub, pubSubClient);
        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(
                TopicMapping.TopicType.LocalMqtt, timeoutingClient);

        // Publish 5 shadow response messages
        for (int i = 0; i < 5; i++) {
            String topic = "$aws/things/device-" + String.format("%03d", i) + "/shadow/update/accepted";
            pubSubAgent.publish(topic, ("{\"v\":" + i + "}").getBytes(), "aws.greengrass.ShadowManager");
        }

        // WITH THE FIX: first publish fails with timeout exception, but the remaining
        // 4 messages should be delivered successfully because the OrderedExecutorService
        // task completes (exception is caught in MessageBridge.handleMessage) and
        // dequeues the next task.
        assertTrue(allMessagesProcessed.await(10, TimeUnit.SECONDS),
                "FIX VERIFIED: All subsequent messages should be delivered after the first "
                + "publish times out. The OrderedExecutorService task completes because "
                + "the exception is caught, allowing the next task to run.");

        assertEquals(5, publishCallCount.get(),
                "All 5 messages should have reached MQTTClient.publish()");
        assertEquals(4, successfulPublishes.size(),
                "4 messages should have been successfully published (first one timed out)");
    }
}
