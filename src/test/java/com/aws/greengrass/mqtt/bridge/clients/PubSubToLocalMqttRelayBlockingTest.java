/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Reproduction test:
 * MQTT Bridge silently stops relaying all Pubsub → LocalMqtt messages
 * when a single synchronous Paho MqttClient.publish() hangs indefinitely
 * (e.g., Moquette drops a PUBACK under high load).
 *
 * The OrderedExecutorService serializes all Pubsub→LocalMqtt tasks per
 * consumer key, so one blocked publish starves all subsequent messages.
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
 * Reproduces the publish deadlock scenario.
 *
 * Production call chain:
 *   ShadowManager → PubSubIPCEventStreamAgent.publish()
 *     → orderedExecutorService.execute(consumer.accept(event), consumer)
 *       → PubSubClient.pubSubCallback → MessageBridge.handleMessage()
 *         → MQTTClient.publish() → Paho Token.waitForResponse() [NO TIMEOUT]
 *
 * This test simulates a dropped PUBACK by making publish() block indefinitely
 * on the first call, then verifies all subsequent messages are starved.
 */
@ExtendWith({MockitoExtension.class, GGExtension.class})
public class PubSubToLocalMqttRelayBlockingTest {

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
    void GIVEN_pubsub_to_localmqtt_relay_WHEN_single_publish_hangs_THEN_all_subsequent_messages_blocked()
            throws Exception {

        // 1. Thread pool + OrderedExecutorService (same as Nucleus production)
        threadPool = Executors.newFixedThreadPool(4);
        OrderedExecutorService orderedExecutorService = new OrderedExecutorService(threadPool);

        // 2. Real PubSubIPCEventStreamAgent (auth not invoked for internal publish)
        PubSubIPCEventStreamAgent pubSubAgent =
                createPubSubAgent(mock(AuthorizationHandler.class), orderedExecutorService);

        // 3. Real PubSubClient
        PubSubClient pubSubClient = new PubSubClient(pubSubAgent);

        // 4. Topic mapping: Pubsub → LocalMqtt
        TopicMapping topicMapping = new TopicMapping();
        topicMapping.updateMapping(Utils.immutableMap(
                "shadow-responses",
                new TopicMapping.MappingEntry(
                        "$aws/things/+/shadow/update/accepted",
                        TopicMapping.TopicType.Pubsub,
                        TopicMapping.TopicType.LocalMqtt)));

        // 5. Real MessageBridge
        MessageBridge messageBridge = new MessageBridge(topicMapping, Collections.emptyMap());

        // 6. Blocking LocalMqtt client — simulates Paho hanging in Token.waitForResponse()
        CountDownLatch firstPublishEntered = new CountDownLatch(1);
        CountDownLatch releaseBlock = new CountDownLatch(1);
        AtomicInteger publishCallCount = new AtomicInteger(0);
        CopyOnWriteArrayList<String> publishedTopics = new CopyOnWriteArrayList<>();

        BlockingMqttClient blockingClient =
                new BlockingMqttClient(firstPublishEntered, releaseBlock, publishCallCount, publishedTopics);

        // 7. Wire the full chain
        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(
                TopicMapping.TopicType.Pubsub, pubSubClient);
        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(
                TopicMapping.TopicType.LocalMqtt, blockingClient);

        // --- Simulate ShadowManager publishing 5 shadow responses ---
        for (int i = 0; i < 5; i++) {
            String topic = "$aws/things/device-" + String.format("%03d", i) + "/shadow/update/accepted";
            pubSubAgent.publish(topic, ("{\"v\":" + i + "}").getBytes(), "aws.greengrass.ShadowManager");
        }

        // --- Verify the deadlock ---

        assertTrue(firstPublishEntered.await(5, TimeUnit.SECONDS),
                "First publish should have entered MQTTClient.publish()");

        // Wait to give OrderedExecutorService time to process queued tasks (it won't)
        Thread.sleep(3000);

        // THE BUG: Only 1 message reached publish(), the rest are stuck in OrderedExecutorService
        assertEquals(1, publishCallCount.get(),
                "BUG REPRODUCED: Only the first message reached MQTTClient.publish(). "
                + "The remaining 4 messages are blocked in OrderedExecutorService "
                + "because OrderedTask.run() only dequeues the next task in its finally{} block, "
                + "which never executes while the first task is blocked. "
                + "This is the silent Pubsub→LocalMqtt relay deadlock.");

        assertTrue(publishedTopics.isEmpty(),
                "No messages were successfully published to LocalMqtt");

        releaseBlock.countDown();
    }

    @Test
    void GIVEN_blocked_pubsub_to_localmqtt_WHEN_localmqtt_to_pubsub_message_sent_THEN_it_still_works()
            throws Exception {

        threadPool = Executors.newFixedThreadPool(4);
        OrderedExecutorService orderedExecutorService = new OrderedExecutorService(threadPool);

        PubSubIPCEventStreamAgent pubSubAgent =
                createPubSubAgent(mock(AuthorizationHandler.class), orderedExecutorService);

        PubSubClient pubSubClient = new PubSubClient(pubSubAgent);

        // Bidirectional mapping
        TopicMapping topicMapping = new TopicMapping();
        topicMapping.updateMapping(Utils.immutableMap(
                "shadow-responses",
                new TopicMapping.MappingEntry(
                        "$aws/things/+/shadow/update/accepted",
                        TopicMapping.TopicType.Pubsub,
                        TopicMapping.TopicType.LocalMqtt),
                "shadow-requests",
                new TopicMapping.MappingEntry(
                        "$aws/things/+/shadow/update",
                        TopicMapping.TopicType.LocalMqtt,
                        TopicMapping.TopicType.Pubsub)));

        MessageBridge messageBridge = new MessageBridge(topicMapping, Collections.emptyMap());

        CountDownLatch firstPublishEntered = new CountDownLatch(1);
        CountDownLatch releaseBlock = new CountDownLatch(1);
        AtomicInteger localMqttPublishCount = new AtomicInteger(0);

        BlockingMqttClient blockingClient =
                new BlockingMqttClient(firstPublishEntered, releaseBlock, localMqttPublishCount, new CopyOnWriteArrayList<>());

        // Track messages arriving on PubSub (LocalMqtt→Pubsub direction)
        CopyOnWriteArrayList<String> pubSubReceivedTopics = new CopyOnWriteArrayList<>();
        CountDownLatch pubSubReceived = new CountDownLatch(1);
        pubSubAgent.subscribe(
                "$aws/things/+/shadow/update",
                event -> {
                    pubSubReceivedTopics.add(event.getTopic());
                    pubSubReceived.countDown();
                },
                "test-subscriber");

        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(
                TopicMapping.TopicType.Pubsub, pubSubClient);
        messageBridge.addOrReplaceMessageClientAndUpdateSubscriptions(
                TopicMapping.TopicType.LocalMqtt, blockingClient);

        // Block the Pubsub→LocalMqtt direction
        pubSubAgent.publish(
                "$aws/things/device-001/shadow/update/accepted",
                "{\"state\":{}}".getBytes(),
                "aws.greengrass.ShadowManager");

        assertTrue(firstPublishEntered.await(5, TimeUnit.SECONDS),
                "Pubsub→LocalMqtt should be blocked");

        // Now simulate a LocalMqtt→Pubsub message (client device sending shadow update)
        // This goes through a completely different code path — Paho callback thread
        // calls messageHandler directly, NOT through OrderedExecutorService
        MqttMessage localMsg = MqttMessage.builder()
                .topic("$aws/things/device-042/shadow/update")
                .payload("{\"state\":{\"desired\":{\"temp\":42}}}".getBytes())
                .build();
        blockingClient.simulateIncomingMessage(localMsg);

        // LocalMqtt→Pubsub should work even though Pubsub→LocalMqtt is blocked
        assertTrue(pubSubReceived.await(5, TimeUnit.SECONDS),
                "LocalMqtt→Pubsub should still work when Pubsub→LocalMqtt is blocked. "
                + "This confirms the two directions are independent.");

        assertEquals("$aws/things/device-042/shadow/update", pubSubReceivedTopics.get(0));

        // Pubsub→LocalMqtt is still stuck
        assertEquals(1, localMqttPublishCount.get());

        releaseBlock.countDown();
    }

    /**
     * A MessageClient that blocks indefinitely on the first publish() call,
     * simulating Paho's Token.waitForResponse() hanging when Moquette drops a PUBACK.
     */
    static class BlockingMqttClient implements MessageClient<MqttMessage> {
        private final CountDownLatch firstPublishEntered;
        private final CountDownLatch releaseBlock;
        private final AtomicInteger publishCallCount;
        private final CopyOnWriteArrayList<String> publishedTopics;
        private volatile Consumer<MqttMessage> messageHandler;

        BlockingMqttClient(CountDownLatch firstPublishEntered, CountDownLatch releaseBlock,
                           AtomicInteger publishCallCount, CopyOnWriteArrayList<String> publishedTopics) {
            this.firstPublishEntered = firstPublishEntered;
            this.releaseBlock = releaseBlock;
            this.publishCallCount = publishCallCount;
            this.publishedTopics = publishedTopics;
        }

        @Override
        public void publish(MqttMessage message) throws MessageClientException {
            int callNum = publishCallCount.incrementAndGet();
            if (callNum == 1) {
                // Simulate Paho Token.waitForResponse() blocking forever
                firstPublishEntered.countDown();
                try {
                    releaseBlock.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return;
            }
            publishedTopics.add(message.getTopic());
        }

        @Override
        public void updateSubscriptions(Set<String> topics, Consumer<MqttMessage> messageHandler) {
            this.messageHandler = messageHandler;
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

        /** Simulate a message arriving from LocalMqtt (as Paho callback thread would deliver it). */
        void simulateIncomingMessage(MqttMessage message) {
            if (messageHandler != null) {
                messageHandler.accept(message);
            }
        }
    }
}
