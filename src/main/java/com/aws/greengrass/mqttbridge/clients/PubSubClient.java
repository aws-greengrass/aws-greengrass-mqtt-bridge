/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge.clients;

import com.aws.greengrass.builtin.services.pubsub.PubSubIPCAgent;
import com.aws.greengrass.ipc.services.pubsub.MessagePublishedEvent;
import com.aws.greengrass.ipc.services.pubsub.PubSubPublishRequest;
import com.aws.greengrass.ipc.services.pubsub.PubSubSubscribeRequest;
import com.aws.greengrass.ipc.services.pubsub.PubSubUnsubscribeRequest;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqttbridge.Message;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import javax.inject.Inject;

public class PubSubClient implements MessageClient {
    private static final Logger LOGGER = LogManager.getLogger(PubSubClient.class);
    public static final String TOPIC = "topic";

    @Getter(AccessLevel.PROTECTED)
    private Set<String> subscribedPubSubTopics = new HashSet<>();

    private Consumer<Message> messageHandler;

    private final PubSubIPCAgent pubSubIPCAgent;

    private final Consumer<MessagePublishedEvent> pubSubCallback = (message) -> {
        String topic = message.getTopic();
        LOGGER.atTrace().kv(TOPIC, topic).log("Received PubSub message");

        if (messageHandler == null) {
            LOGGER.atWarn().kv(TOPIC, topic).log("PubSub message received but message handler not set");
        } else {
            Message msg = new Message(topic, message.getPayload());
            messageHandler.accept(msg);
        }
    };

    /**
     * Constructor for PubSubClient.
     *
     * @param pubSubIPCAgent for interacting with PubSub
     */
    @Inject
    public PubSubClient(PubSubIPCAgent pubSubIPCAgent) {
        this.pubSubIPCAgent = pubSubIPCAgent;
    }

    /**
     * Start the {@link PubSubClient}.
     */
    public void start() {
    }

    /**
     * Stop the {@link PubSubClient}.
     */
    public void stop() {
        removeMappingAndSubscriptions();
    }

    private synchronized void removeMappingAndSubscriptions() {
        unsubscribeAll();
        subscribedPubSubTopics.clear();
    }

    private void unsubscribeAll() {
        LOGGER.atDebug().kv("mapping", subscribedPubSubTopics).log("unsubscribe from pubsub topics");

        this.subscribedPubSubTopics.forEach(s -> {
            unsubscribeFromPubSub(s);
            LOGGER.atDebug().kv(TOPIC, s).log("Unsubscribed to topic");
        });
    }

    private void unsubscribeFromPubSub(String topic) {
        PubSubUnsubscribeRequest unsubscribeRequest = PubSubUnsubscribeRequest.builder().topic(topic).build();
        pubSubIPCAgent.unsubscribe(unsubscribeRequest, pubSubCallback);
    }

    @Override
    public void publish(Message message) {
        publishToPubSub(message.getTopic(), message.getPayload());
    }

    private void publishToPubSub(String topic, byte[] payload) {
        PubSubPublishRequest publishRequest = PubSubPublishRequest.builder().topic(topic).payload(payload).build();
        pubSubIPCAgent.publish(publishRequest);
    }

    @Override
    public synchronized void updateSubscriptions(Set<String> topics, @NonNull Consumer<Message> messageHandler) {
        this.messageHandler = messageHandler;
        LOGGER.atDebug().kv("topics", topics).log("Subscribing to pubsub topics");

        Set<String> topicsToRemove = new HashSet<>(subscribedPubSubTopics);
        topicsToRemove.removeAll(topics);
        topicsToRemove.forEach(s -> {
            unsubscribeFromPubSub(s);
            LOGGER.atDebug().kv(TOPIC, s).log("Unsubscribed to topic");
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
        PubSubSubscribeRequest subscribeRequest = PubSubSubscribeRequest.builder().topic(topic).build();
        pubSubIPCAgent.subscribe(subscribeRequest, pubSubCallback);
    }
}
