/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.iot.evergreen.mqtt.bridge;

import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Bridges the messages flowing between clients to various brokers. This class implements a simple observer pattern.
 * Anyone interested in messages of certain source type can listen for those messages Example, IoTCore client will be
 * interested in messages with source as LocalMqtt, so it can listen for messages with sourceType as LocalMqtt
 * messageBridge.addListener(listener, TopicMapping.TopicType.LocalMqtt)
 */
@NoArgsConstructor
public class MessageBridge {
    private Map<TopicMapping.TopicType, List<MessageListener>> listeners = new ConcurrentHashMap<>();

    /**
     * Add a listener to listen to messages of type sourceType.
     *
     * @param listener   listener to add
     * @param sourceType type of messages to listen to
     */
    public void addListener(MessageListener listener, TopicMapping.TopicType sourceType) {
        listeners.compute(sourceType, (topicType, messageListeners) -> {
            if (messageListeners == null) {
                List<MessageListener> newMessageListeners = new ArrayList<>();
                newMessageListeners.add(listener);
                return newMessageListeners;
            } else {
                messageListeners.add(listener);
                return messageListeners;
            }
        });
    }

    /**
     * Remove a listener listening for message of given type sourceType.
     *
     * @param listener   listener to remove
     * @param sourceType type the listener is listener is registered to listen
     */
    public void removeListener(MessageListener listener, TopicMapping.TopicType sourceType) {
        listeners.computeIfPresent(sourceType, (topicType, messageListeners) -> {
            messageListeners.remove(listener);
            if (messageListeners.isEmpty()) {
                return null;
            }
            return messageListeners;
        });
    }

    /**
     * Notify listeners of a message from source sourceType.
     *
     * @param msg        message to be notified about
     * @param sourceType type of source
     */
    public void notifyMessage(Message msg, TopicMapping.TopicType sourceType) {
        List<MessageListener> messageListeners = listeners.get(sourceType);
        if (messageListeners != null) {
            messageListeners.forEach(listener -> listener.onMessage(sourceType, msg));
        }
    }

    /**
     * Listener to be notified of messages.
     */
    interface MessageListener {
        void onMessage(TopicMapping.TopicType sourceType, Message msg);
    }
}
