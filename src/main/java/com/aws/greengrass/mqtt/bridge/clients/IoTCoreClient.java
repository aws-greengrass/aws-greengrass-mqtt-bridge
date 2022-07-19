/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.clients;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqtt.bridge.Message;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import javax.inject.Inject;

public class IoTCoreClient implements MessageClient {
    private static final Logger LOGGER = LogManager.getLogger(IoTCoreClient.class);
    public static final String TOPIC = "topic";
    private final RetryUtils.RetryConfig subscribeRetryConfig =
            RetryUtils.RetryConfig.builder().initialRetryInterval(Duration.ofSeconds(1L))
                    .maxRetryInterval(Duration.ofSeconds(120L)).maxAttempt(Integer.MAX_VALUE)
                    .retryableExceptions(Arrays.asList(ExecutionException.class, TimeoutException.class)).build();

    @Getter(AccessLevel.PROTECTED)
    private Set<String> subscribedIotCoreTopics = ConcurrentHashMap.newKeySet();
    @Getter(AccessLevel.PROTECTED)
    private Set<String> toSubscribeIotCoreTopics = new HashSet<>();
    private volatile Consumer<Message> messageHandler;
    @Getter(AccessLevel.PACKAGE) // for unit testing
    private Future<?> subscribeFuture;
    private final Object subscribeLock = new Object();
    private final AtomicBoolean connectionInterrupted = new AtomicBoolean();
    private final MqttClient iotMqttClient;
    private final ExecutorService executorService;

    private final Consumer<MqttMessage> iotCoreCallback = (message) -> {
        String topic = message.getTopic();
        LOGGER.atTrace().kv(TOPIC, topic).log("Received IoT Core message");

        Consumer<Message> handler = messageHandler;
        if (handler == null) {
            LOGGER.atWarn().kv(TOPIC, topic).log("IoT Core message received but message handler not set");
        } else {
            Message msg = new Message(topic, message.getPayload());
            handler.accept(msg);
        }
    };

    @Getter(AccessLevel.PACKAGE) // for unit testing
    private final MqttClientConnectionEvents connectionCallbacks = new MqttClientConnectionEvents() {
        @Override
        public void onConnectionInterrupted(int errorCode) {
            connectionInterrupted.set(true);
        }

        @Override
        public void onConnectionResumed(boolean sessionPresent) {
            connectionInterrupted.set(false);
            synchronized (subscribeLock)  {
                if (subscribeFuture != null) {
                    subscribeFuture.cancel(true);
                }
                // subscribe to any topics left to be subscribed
                Set<String> topicsToSubscribe = new HashSet<>(toSubscribeIotCoreTopics);
                topicsToSubscribe.removeAll(subscribedIotCoreTopics);
                subscribeFuture = executorService.submit(() -> subscribeToTopicsWithRetry(topicsToSubscribe));
            }
        }
    };

    /**
     * Constructor for IoTCoreClient.
     *
     * @param iotMqttClient for interacting with IoT Core
     * @param executorService for tasks asynchronously
     */
    @Inject
    public IoTCoreClient(MqttClient iotMqttClient, ExecutorService executorService) {
        this.iotMqttClient = iotMqttClient;
        this.executorService = executorService;
        iotMqttClient.addToCallbackEvents(connectionCallbacks);
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

    private void removeMappingAndSubscriptions() {
        unsubscribeAll();
        subscribedIotCoreTopics.clear();
    }

    private void unsubscribeAll() {
        LOGGER.atDebug().kv("mapping", subscribedIotCoreTopics).log("Unsubscribe from IoT Core topics");

        this.subscribedIotCoreTopics.forEach(s -> {
            try {
                unsubscribeFromIotCore(s);
                LOGGER.atDebug().kv(TOPIC, s).log("Unsubscribed from topic");
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
    public void updateSubscriptions(Set<String> topics, @NonNull Consumer<Message> messageHandler) {
        synchronized (subscribeLock) {
            if (subscribeFuture != null) {
                subscribeFuture.cancel(true);
            }

            this.messageHandler = messageHandler;
            this.toSubscribeIotCoreTopics = new HashSet<>(topics);
            LOGGER.atDebug().kv("topics", topics).log("Subscribing to IoT Core topics");

            Set<String> topicsToRemove = new HashSet<>(subscribedIotCoreTopics);
            topicsToRemove.removeAll(topics);
            topicsToRemove.forEach(s -> {
                try {
                    unsubscribeFromIotCore(s);
                    LOGGER.atDebug().kv(TOPIC, s).log("Unsubscribed from topic");
                    subscribedIotCoreTopics.remove(s);
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    LOGGER.atError().kv(TOPIC, s).setCause(e).log("Unable to unsubscribe");
                    // If we are unable to unsubscribe, leave the topic in the set
                    // so that we can try to remove next time.
                }
            });

            Set<String> topicsToSubscribe = new HashSet<>(topics);
            topicsToSubscribe.removeAll(subscribedIotCoreTopics);

            subscribeFuture = executorService.submit(() -> subscribeToTopicsWithRetry(topicsToSubscribe));
        }
    }

    @Override
    public boolean supportsTopicFilters() {
        return true;
    }

    @SuppressWarnings({"PMD.AvoidCatchingGenericException", "PMD.PreserveStackTrace", "PMD.ExceptionAsFlowControl"})
    private void subscribeToTopicsWithRetry(Set<String> topics) {
        topics.forEach(s -> {
            try {
                RetryUtils.runWithRetry(subscribeRetryConfig, () -> {
                    try {
                        // Only subscribe if mqtt client is connected.
                        //
                        // If the subscription fails due to network reasons,
                        // we have several methods of retrying:
                        //
                        // 1) general case: when the mqtt client connection has been interrupted,
                        //    we rely on connectionCallbacks#onConnectionResumed handler
                        //    to retry the work.
                        //
                        // 2) edge case: when bridge starts offline. we can't rely on the reconnection handler since
                        //    the subscriptions were never made yet, so we must retry in this method.
                        if (!connectionInterrupted.get()) {
                            subscribeToIotCore(s);
                            subscribedIotCoreTopics.add(s);
                        }
                        // useless return
                        return null;
                    } catch (ExecutionException e) {
                        Throwable cause = e.getCause();
                        if (cause instanceof InterruptedException) {
                            throw (InterruptedException) cause;
                        } else {
                            throw e;
                        }
                    }
                }, "subscribe-iotcore-topic", LOGGER);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                LOGGER.atError().kv(TOPIC, s).setCause(e).log("Failed to subscribe to IoTCore topic");
            }
        });
    }

    private void subscribeToIotCore(String topic) throws InterruptedException, ExecutionException, TimeoutException {
        SubscribeRequest subscribeRequest = SubscribeRequest.builder().topic(topic).callback(iotCoreCallback)
                .qos(QualityOfService.AT_LEAST_ONCE).build();
        iotMqttClient.subscribe(subscribeRequest);
    }
}
