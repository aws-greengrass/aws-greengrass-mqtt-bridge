/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.clients;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqtt.bridge.model.Message;
import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.mqttclient.MqttRequestException;
import com.aws.greengrass.mqttclient.PublishRequest;
import com.aws.greengrass.mqttclient.v5.Publish;
import com.aws.greengrass.mqttclient.v5.QOS;
import com.aws.greengrass.mqttclient.v5.Subscribe;
import com.aws.greengrass.mqttclient.v5.SubscribeResponse;
import com.aws.greengrass.mqttclient.v5.Unsubscribe;
import com.aws.greengrass.util.Utils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import software.amazon.awssdk.crt.mqtt.MqttClientConnectionEvents;
import software.amazon.awssdk.crt.mqtt.QualityOfService;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import javax.inject.Inject;

public class IoTCoreClient implements MessageClient<com.aws.greengrass.mqtt.bridge.model.MqttMessage> {
    private static final Logger LOGGER = LogManager.getLogger(IoTCoreClient.class);
    private static final String LOG_KEY_TOPIC = "topic";
    private static final String LOG_KEY_TOPICS = "topics";

    private final SubscriptionManager subscriptionManager = new SubscriptionManager();
    private volatile Consumer<com.aws.greengrass.mqtt.bridge.model.MqttMessage> messageHandler;

    private final MqttClient iotMqttClient;
    private final ExecutorService executorService;

    private final Consumer<Publish> iotCoreCallback = (message) -> {
        String topic = message.getTopic();
        LOGGER.atTrace().kv(LOG_KEY_TOPIC, topic).log("Received IoT Core message");

        Consumer<com.aws.greengrass.mqtt.bridge.model.MqttMessage> handler = messageHandler;
        if (handler == null) {
            LOGGER.atWarn().kv(LOG_KEY_TOPIC, topic).log("IoT Core message received but message handler not set");
        } else {
            handler.accept(com.aws.greengrass.mqtt.bridge.model.MqttMessage.fromSpoolerV5Model(message));
        }
    };

    @Getter(AccessLevel.PACKAGE) // for unit testing
    private final MqttClientConnectionEvents connectionCallbacks = new MqttClientConnectionEvents() {
        @Override
        public void onConnectionInterrupted(int errorCode) {
            subscriptionManager.offline();
        }

        @Override
        public void onConnectionResumed(boolean sessionPresent) {
            subscriptionManager.online();
            subscriptionManager.subscribeAll();
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
        // onConnect handler required to handle case when bridge starts offline
        iotMqttClient.addToCallbackEvents(connectionCallbacks::onConnectionResumed, connectionCallbacks);
    }

    /**
     * Start the {@link IoTCoreClient}.
     */
    @Override
    public void start() {
    }

    /**
     * Stop the {@link IoTCoreClient}.
     */
    @Override
    public void stop() {
        subscriptionManager.shutdown(1L, TimeUnit.MINUTES);
    }

    private CompletableFuture<Void> unsubscribe(String topic) {
        try {
            return iotMqttClient.unsubscribe(Unsubscribe.builder()
                    .topic(topic)
                    .subscriptionCallback(iotCoreCallback)
                    .build());
        } catch (MqttRequestException e) {
            CompletableFuture<Void> result = new CompletableFuture<>();
            result.completeExceptionally(e);
            return result;
        }
    }

    @Override
    public void publish(com.aws.greengrass.mqtt.bridge.model.MqttMessage message) {
        PublishRequest publishRequest = PublishRequest.builder()
                .topic(message.getTopic())
                .payload(message.getPayload())
                .qos(QualityOfService.AT_LEAST_ONCE)
                .build();
        // pubacks are not propagated
        iotMqttClient.publish(publishRequest).exceptionally(e -> {
            LOGGER.atWarn().cause(e).kv(LOG_KEY_TOPIC, message.getTopic())
                    .log("Failed to publish message");
            return null;
        });
    }

    @Override
    public void updateSubscriptions(Set<String> topics,
                                    @NonNull Consumer<com.aws.greengrass.mqtt.bridge.model.MqttMessage>
                                            messageHandler) {
        this.messageHandler = messageHandler;
        subscriptionManager.replaceSubscriptions(topics);
    }

    @Override
    public boolean supportsTopicFilters() {
        return true;
    }

    private CompletableFuture<SubscribeResponse> subscribe(String topic) {
            try {
                return iotMqttClient.subscribe(Subscribe.builder()
                        .topic(topic)
                        .callback(iotCoreCallback)
                        // TODO support noLocal
                        // TODO support retainAsPublished
                        .qos(QOS.AT_LEAST_ONCE)
                        // TODO support retainHandlingType, different for mqtt5 vs mqtt3?
                        .build());
            } catch (MqttRequestException e) {
                CompletableFuture<SubscribeResponse> result = new CompletableFuture<>();
                result.completeExceptionally(e);
                return result;
            }
    }

    private static boolean subscriptionIsSuccessful(SubscribeResponse resp) {
        return resp.getReasonCode() < 3; // GRANTED QOS 0/1/2 or SUCCESS
    }

    @Override
    public com.aws.greengrass.mqtt.bridge.model.MqttMessage convertMessage(Message message) {
        return (com.aws.greengrass.mqtt.bridge.model.MqttMessage) message.toMqtt();
    }

    // for unit testing
    Set<String> getSubscribed() {
        return subscriptionManager.getSubscribed();
    }

    // for unit testing
    Set<String> getToSubscribe() {
        return subscriptionManager.getToSubscribe();
    }

    // TODO factor out so can be used by local v5 client
    class SubscriptionManager {
        private final Object subscriptionLock = new Object();
        private final Set<String> subscribed = new HashSet<>();
        private final Set<String> toSubscribe = new HashSet<>();
        private final AtomicBoolean online = new AtomicBoolean(true);
        private final AtomicBoolean shutdown = new AtomicBoolean(false);

        /**
         * Stop accepting new subscription requests, cancel all in-progress subscriptions,
         * and unsubscribe.
         */
        public void shutdown(long timeout, TimeUnit unit) {
            if (!shutdown.compareAndSet(false, true)) {
                return;
            }
            try {
                subscriptionManager.unsubscribeAll().get(timeout, unit);
            } catch (ExecutionException e) {
                LOGGER.atWarn().cause(Utils.getUltimateCause(e)).log("unable to unsubscribe from topics");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (TimeoutException e) {
                LOGGER.atWarn().cause(Utils.getUltimateCause(e)).log("timed-out when unsubscribing from topics");
            }
        }

        /**
         * Subscribe to all known topics. All previously failed/not-made subscriptions will
         * attempt to subscribed again. If a topic has already been subscribed to, it will not be
         * resubscribed.
         */
        public CompletableFuture<Void> subscribeAll() {
            Set<String> topicsToSubscribe;
            synchronized (subscriptionLock) {
                topicsToSubscribe = new HashSet<>(toSubscribe);
                topicsToSubscribe.removeAll(subscribed);
            }
            return subscribe(topicsToSubscribe);
        }

        public CompletableFuture<Void> subscribe(Collection<String> topics) {
            CompletableFuture<Void> result = CompletableFuture.supplyAsync(() -> null, executorService);
            for (String topic : topics) {
                result.thenComposeAsync(unused -> {
                    if (canSubscribe()) {
                        return subscribeWithRetry(topic);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                }, executorService);
            }
            return result;
        }

        private CompletableFuture<Void> subscribeWithRetry(String topic) {
            return IoTCoreClient.this.subscribe(topic).thenComposeAsync(resp -> {
                        CompletableFuture<Void> subscribeResult =
                                CompletableFuture.supplyAsync(() -> null, executorService);
                        if (subscriptionIsSuccessful(resp)) {
                            synchronized (subscriptionLock) {
                                subscribed.add(topic);
                            }
                            subscribeResult.complete(null);
                        } else {
                            LOGGER.atWarn().kv("reasonCode", resp.getReasonCode()).kv(LOG_KEY_TOPIC, topic)
                                    .log("subscription failed");
                            // TODO better exception?
                            subscribeResult.completeExceptionally(
                                    new MessageClientException("Subscription failed"));
                        }
                        return subscribeResult;
                    }, executorService)
                    .exceptionally(e -> {
                        if (canSubscribe()) {
                            return subscribeWithRetry(topic).join(); // TODO is this join ok?
                        }
                        return null;
                    });
        }

        /**
         * Unsubscribe from all known topics.
         */
        public CompletableFuture<Void> unsubscribeAll() {
            Set<String> toUnsubscribe;
            synchronized (subscriptionLock) {
                toUnsubscribe = new HashSet<>(subscribed);
            }
            return unsubscribeTopics(toUnsubscribe)
                    .thenAcceptAsync(unused -> {
                        synchronized (subscriptionLock) {
                            subscribed.clear();
                        }
                    }, executorService);
        }

        public CompletableFuture<Void> unsubscribe(Collection<String> topics) {
            return unsubscribeTopics(topics);
        }

        private CompletableFuture<Void> unsubscribeTopics(Collection<String> topics) {
            LOGGER.atDebug().kv(LOG_KEY_TOPICS, topics).log("Unsubscribe from IoT Core topics");
            return CompletableFuture.allOf(topics.stream()
                    .map(topic -> {
                        if (canUnsubscribe()) {
                            return IoTCoreClient.this.unsubscribe(topic).thenApplyAsync(unused -> {
                                LOGGER.atDebug().kv(LOG_KEY_TOPIC, topic).log("Unsubscribed from topic");
                                synchronized (subscriptionLock) {
                                    subscribed.remove(topic);
                                }
                                return null;
                            }, executorService);
                        } else {
                            return null;
                        }
                    })
                    .toArray(CompletableFuture[]::new));
        }

        /**
         * Subscribe to the provided topics.  If we're subscribed to extra topics not defined in this list, they will
         * be unsubscribed.
         * @param topics topics to subscribe to
         */
        public CompletableFuture<Void> replaceSubscriptions(Collection<String> topics) {
            if (shutdown.get()) {
                return CompletableFuture.completedFuture(null);
            }

            synchronized (subscriptionLock) {
                toSubscribe.clear();
                toSubscribe.addAll(new HashSet<>(topics));
            }

            if (!online.get()) {
                // make sure that we record toSubscribe topics before giving up
                return CompletableFuture.completedFuture(null);
            }

            LOGGER.atDebug().kv(LOG_KEY_TOPICS, topics).log("Subscribing to IoT Core topics");

            Set<String> topicsToRemove;
            synchronized (subscriptionLock) {
                topicsToRemove = new HashSet<>(subscribed);
            }
            topicsToRemove.removeAll(topics);

            return unsubscribe(topicsToRemove)
                    .thenComposeAsync(unused -> {
                        Set<String> topicsToSubscribe = new HashSet<>(topics);
                        synchronized (subscriptionLock) {
                            topicsToSubscribe.removeAll(subscribed);
                        }
                        return subscribe(topicsToSubscribe);
                    }, executorService);
        }

        public void offline() {
            online.set(false);
        }

        public void online() {
            online.set(true);
        }

        private void checkOnline() {
            online.set(iotMqttClient.connected());
        }

        private boolean canSubscribe() {
            checkOnline();
            return online.get() && !shutdown.get();
        }

        private boolean canUnsubscribe() {
            checkOnline();
            return online.get();
        }

        // for unit testing
        Set<String> getSubscribed() {
            Set<String> subscribed;
            synchronized (subscriptionLock) {
                subscribed = new HashSet<>(this.subscribed);
            }
            return subscribed;
        }

        // for unit testing
        Set<String> getToSubscribe() {
            Set<String> toSubscribe;
            synchronized (subscriptionLock) {
                toSubscribe = new HashSet<>(this.toSubscribe);
            }
            return toSubscribe;
        }
    }
}
