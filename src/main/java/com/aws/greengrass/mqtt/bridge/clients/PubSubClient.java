/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.clients;

import com.aws.greengrass.builtin.services.pubsub.PubSubIPCEventStreamAgent;
import com.aws.greengrass.builtin.services.pubsub.PublishEvent;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqtt.bridge.MQTTBridge;
import com.aws.greengrass.mqtt.bridge.model.Message;
import com.aws.greengrass.mqtt.bridge.model.PubSubMessage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import javax.inject.Inject;

public class PubSubClient implements MessageClient<PubSubMessage> {
    private static final Logger LOGGER = LogManager.getLogger(PubSubClient.class);
    public static final String TOPIC = "topic";

    @Getter(AccessLevel.PROTECTED)
    private Set<String> subscribedPubSubTopics = new HashSet<>();

    private Consumer<PubSubMessage> messageHandler;

    private final PubSubIPCEventStreamAgent pubSubIPCAgent;

    private final Consumer<PublishEvent> pubSubCallback = (message) -> {
        String topic = message.getTopic();
        LOGGER.atTrace().kv(TOPIC, topic).log("Received local pub/sub message");

        if (messageHandler == null) {
            LOGGER.atWarn().kv(TOPIC, topic).log("Local pub/sub message received but message handler not set");
        } else {
            messageHandler.accept(PubSubMessage.fromPublishEvent(message));
        }
    };

    /**
     * Constructor for PubSubClient.
     *
     * @param pubSubIPCAgent for interacting with PubSub
     */
    @Inject
    public PubSubClient(PubSubIPCEventStreamAgent pubSubIPCAgent) {
        this.pubSubIPCAgent = pubSubIPCAgent;
    }

    /**
     * Start the {@link PubSubClient}.
     */
    @Override
    public void start() {
    }

    /**
     * Stop the {@link PubSubClient}.
     */
    @Override
    public void stop() {
        removeMappingAndSubscriptions();
    }

    private synchronized void removeMappingAndSubscriptions() {
        unsubscribeAll();
        subscribedPubSubTopics.clear();
    }

    private void unsubscribeAll() {
        LOGGER.atDebug().kv("mapping", subscribedPubSubTopics).log("Unsubscribe from local pub/sub topics");

        this.subscribedPubSubTopics.forEach(s -> {
            unsubscribeFromPubSub(s);
            LOGGER.atDebug().kv(TOPIC, s).log("Unsubscribed from topic");
        });
    }

    private void unsubscribeFromPubSub(String topic) {
        pubSubIPCAgent.unsubscribe(topic, pubSubCallback, MQTTBridge.SERVICE_NAME);
    }

    @Override
    public void publish(PubSubMessage message) {
        pubSubIPCAgent.publish(message.getTopic(), message.getPayload(), MQTTBridge.SERVICE_NAME);
    }

    @Override
    public synchronized void updateSubscriptions(Set<String> topics, @NonNull Consumer<PubSubMessage> messageHandler) {
        this.messageHandler = messageHandler;
        LOGGER.atDebug().kv("topics", topics).log("Subscribing to local pub/sub topics");

        Set<String> topicsToRemove = new HashSet<>(subscribedPubSubTopics);
        topicsToRemove.removeAll(topics);
        topicsToRemove.forEach(s -> {
            unsubscribeFromPubSub(s);
            LOGGER.atDebug().kv(TOPIC, s).log("Unsubscribed from topic");
            subscribedPubSubTopics.remove(s);
        });

        Set<String> topicsToSubscribe = new HashSet<>(topics);
        topicsToSubscribe.removeAll(subscribedPubSubTopics);

        topicsToSubscribe.forEach(s -> {
            subscribeToPubSub(s);
            LOGGER.atDebug().kv(TOPIC, s).log("Subscribed to topic");
            subscribedPubSubTopics.add(s);
        });
    }

    private void subscribeToPubSub(String topic) {
        pubSubIPCAgent.subscribe(topic, pubSubCallback, MQTTBridge.SERVICE_NAME);
    }

    @Override
    public PubSubMessage convertMessage(Message message) {
        return (PubSubMessage) message.toPubSub();
    }
}
