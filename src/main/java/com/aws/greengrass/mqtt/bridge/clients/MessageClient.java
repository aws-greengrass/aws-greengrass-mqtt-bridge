/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.clients;

import com.aws.greengrass.mqtt.bridge.model.Message;

import java.util.Set;
import java.util.function.Consumer;

public interface MessageClient<T extends Message> {
    /**
     * Publish the message.
     *
     * @param message Message to publish
     * @throws MessageClientException if fails to publish the message
     */
    void publish(T message) throws MessageClientException;

    // TODO: Add QoS support

    /**
     * Update subscriptions to the given set of topics. Unsubscribe from subscribed topics missing from the given set
     *
     * @param topics         topics to subscribe
     * @param messageHandler handler to call when message is received on the subscription
     */
    void updateSubscriptions(Set<String> topics, Consumer<T> messageHandler);

    T convertMessage(Message message);

    void start();

    void stop();
}
