/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge.clients;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqttbridge.Message;
import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.mqttclient.PublishRequest;
import com.aws.greengrass.mqttclient.SubscribeRequest;
import com.aws.greengrass.mqttclient.UnsubscribeRequest;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import software.amazon.awssdk.crt.mqtt.MqttMessage;
import software.amazon.awssdk.crt.mqtt.QualityOfService;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import javax.inject.Inject;

public class IoTCoreClient implements MessageClient {
    private static final Logger LOGGER = LogManager.getLogger(IoTCoreClient.class);
    public static final String TOPIC = "topic";

    @Getter(AccessLevel.PROTECTED)
    private Set<String> subscribedIotCoreTopics = new HashSet<>();

    private Consumer<Message> messageHandler;

    private final MqttClient iotMqttClient;

    private final Consumer<MqttMessage> iotCoreCallback = (message) -> {
        String topic = message.getTopic();
        LOGGER.atTrace().kv(TOPIC, topic).log("Received IoTCore message");

        if (messageHandler == null) {
            LOGGER.atWarn().kv(TOPIC, topic).log("IoTCore message received but message handler not set");
        } else {
            Message msg = new Message(topic, message.getPayload());
            messageHandler.accept(msg);
        }
    };

    /**
     * Constructor for IoTCoreClient.
     *
     * @param iotMqttClient for interacting with IoT Core
     */
    @Inject
    public IoTCoreClient(MqttClient iotMqttClient) {
        this.iotMqttClient = iotMqttClient;
    }

    /**
     * Start the {@link IoTCoreClient}.
     */
    public void start() {
    }

    /**
     * Stop the {@link IoTCoreClient}.
     */
    public void stop() {
        removeMappingAndSubscriptions();
    }

    private synchronized void removeMappingAndSubscriptions() {
        unsubscribeAll();
        subscribedIotCoreTopics.clear();
    }

    private void unsubscribeAll() {
        LOGGER.atDebug().kv("mapping", subscribedIotCoreTopics).log("unsubscribe from iot core topics");

        this.subscribedIotCoreTopics.forEach(s -> {
            try {
                unsubscribeFromIotCore(s);
                LOGGER.atDebug().kv(TOPIC, s).log("Unsubscribed to topic");
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                LOGGER.atWarn().kv(TOPIC, s).setCause(e).log("Unable to unsubscribe");
            }
        });
    }

    private void unsubscribeFromIotCore(String topic)
            throws InterruptedException, ExecutionException, TimeoutException {
        UnsubscribeRequest unsubscribeRequest = UnsubscribeRequest.builder().topic(topic).callback(iotCoreCallback)
                .build();
        iotMqttClient.unsubscribe(unsubscribeRequest);
    }

    @Override
    public void publish(Message message) {
        publishToIotCore(message.getTopic(), message.getPayload());
    }

    private void publishToIotCore(String topic, byte[] payload) {
        PublishRequest publishRequest = PublishRequest.builder().topic(topic).payload(payload)
                .qos(QualityOfService.AT_LEAST_ONCE).build();
        iotMqttClient.publish(publishRequest);
    }

    @Override
    public synchronized void updateSubscriptions(Set<String> topics, @NonNull Consumer<Message> messageHandler) {
        this.messageHandler = messageHandler;
        LOGGER.atDebug().kv("topics", topics).log("Subscribing to IoT Core topics");

        Set<String> topicsToRemove = new HashSet<>(subscribedIotCoreTopics);
        topicsToRemove.removeAll(topics);
        topicsToRemove.forEach(s -> {
            try {
                unsubscribeFromIotCore(s);
                LOGGER.atDebug().kv(TOPIC, s).log("Unsubscribed to topic");
                subscribedIotCoreTopics.remove(s);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                LOGGER.atError().kv(TOPIC, s).setCause(e).log("Unable to unsubscribe");
                // If we are unable to unsubscribe, leave the topic in the set so that we can try to remove next time.
            }
        });

        Set<String> topicsToSubscribe = new HashSet<>(topics);
        topicsToSubscribe.removeAll(subscribedIotCoreTopics);

        topicsToSubscribe.forEach(s -> {
            try {
                subscribeToIotCore(s);
                LOGGER.atDebug().kv(TOPIC, s).log("Subscribed to topic");
                subscribedIotCoreTopics.add(s);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                LOGGER.atError().kv(TOPIC, s).log("Failed to subscribe");
            }
        });
    }

    private void subscribeToIotCore(String topic) throws InterruptedException, ExecutionException, TimeoutException {
        SubscribeRequest subscribeRequest = SubscribeRequest.builder().topic(topic).callback(iotCoreCallback)
                .qos(QualityOfService.AT_LEAST_ONCE).build();
        iotMqttClient.subscribe(subscribeRequest);
    }
}
