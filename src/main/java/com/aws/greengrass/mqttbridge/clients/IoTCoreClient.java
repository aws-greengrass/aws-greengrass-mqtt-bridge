/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge.clients;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqttbridge.Message;
import com.aws.greengrass.mqttclient.CallbackEventManager;
import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.mqttclient.PublishRequest;
import com.aws.greengrass.mqttclient.SubscribeRequest;
import com.aws.greengrass.mqttclient.UnsubscribeRequest;
import com.aws.greengrass.util.RetryUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import software.amazon.awssdk.crt.mqtt.MqttClientConnectionEvents;
import software.amazon.awssdk.crt.mqtt.MqttMessage;
import software.amazon.awssdk.crt.mqtt.QualityOfService;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import javax.inject.Inject;

public class IoTCoreClient implements MessageClient {
    private static final Logger LOGGER = LogManager.getLogger(IoTCoreClient.class);
    public static final String TOPIC = "topic";

    @Getter(AccessLevel.PROTECTED)
    private Set<String> subscribedIotCoreTopics = ConcurrentHashMap.newKeySet();

    private Set<String> toSubscribeIotCoreTopics = new HashSet<>();

    private Future<?> subscribeRetryFuture;

    private Consumer<Message> messageHandler;

    private final MqttClient iotMqttClient;

    private final ExecutorService executorService;

    private final RetryUtils.RetryConfig subscribeRetryConfig = RetryUtils.RetryConfig.builder()
            .initialRetryInterval(Duration.ofMinutes(1L)).maxRetryInterval(Duration.ofMinutes(30L))
            .maxAttempt(Integer.MAX_VALUE)
            .retryableExceptions(Arrays.asList(ExecutionException.class, TimeoutException.class)).build();

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

    private final MqttClientConnectionEvents connectionCallback = new MqttClientConnectionEvents() {
        @Override
        public void onConnectionInterrupted(int i) {
            LOGGER.atDebug().log("Connection to cloud interrupted. Cancelling subscription update");
            if (subscribeRetryFuture != null) {
                subscribeRetryFuture.cancel(true);
            }
        }

        @Override
        public void onConnectionResumed(boolean b) {
            LOGGER.atDebug().log("Connection to cloud resumed. Updating subscriptions");
            startNewSubscribeRetryWorkflow();
        }
    };

    private final CallbackEventManager.OnConnectCallback onConnect = connectionCallback::onConnectionResumed;

    /**
     * Constructor for IoTCoreClient.
     *
     * @param iotMqttClient   for interacting with IoT Core
     * @param executorService for updating subscriptions on separate thread
     */
    @Inject
    public IoTCoreClient(MqttClient iotMqttClient, ExecutorService executorService) {
        this.iotMqttClient = iotMqttClient;
        this.iotMqttClient.addToCallbackEvents(onConnect, connectionCallback);
        this.executorService = executorService;
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
        if (subscribeRetryFuture != null) {
            subscribeRetryFuture.cancel(true);
        }
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

        this.toSubscribeIotCoreTopics = topics;
        LOGGER.atDebug().kv("topics", topics).log("Updated IoT Core topics to subscribe");

        if (iotMqttClient.connected()) {
            startNewSubscribeRetryWorkflow();
        }
    }

    private synchronized void updateSubscriptionsInternal()
            throws ExecutionException, TimeoutException, InterruptedException {
        Set<String> topicsToRemove = new HashSet<>(subscribedIotCoreTopics);
        topicsToRemove.removeAll(toSubscribeIotCoreTopics);
        for (String s : topicsToRemove) {
            unsubscribeFromIotCore(s);
            LOGGER.atDebug().kv(TOPIC, s).log("Unsubscribed to topic");
            subscribedIotCoreTopics.remove(s);
        }

        Set<String> topicsToSubscribe = new HashSet<>(toSubscribeIotCoreTopics);
        topicsToSubscribe.removeAll(subscribedIotCoreTopics);

        for (String s : topicsToSubscribe) {
            subscribeToIotCore(s);
            LOGGER.atDebug().kv(TOPIC, s).log("Subscribed to topic");
            subscribedIotCoreTopics.add(s);
        }
    }

    private void subscribeToIotCore(String topic) throws InterruptedException, ExecutionException, TimeoutException {
        SubscribeRequest subscribeRequest = SubscribeRequest.builder().topic(topic).callback(iotCoreCallback)
                .qos(QualityOfService.AT_LEAST_ONCE).build();
        iotMqttClient.subscribe(subscribeRequest);
    }

    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    private synchronized void startNewSubscribeRetryWorkflow() {
        if (subscribeRetryFuture != null) {
            subscribeRetryFuture.cancel(true);
        }
        subscribeRetryFuture = executorService.submit(() -> {
            try {
                RetryUtils.runWithRetry(subscribeRetryConfig, () -> {
                    updateSubscriptionsInternal();
                    return null;
                }, "update-subscriptions", LOGGER);
            } catch (InterruptedException e) {
                LOGGER.atDebug().setCause(e).log("Retry workflow interrupted");
            } catch (Exception e) {
                LOGGER.atError().setCause(e).log("Failed to update subscriptions");
            }
        });
    }
}
