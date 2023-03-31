/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.clients;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqtt.bridge.model.Message;
import com.aws.greengrass.mqtt.bridge.model.Mqtt5RouteOptions;
import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.mqttclient.MqttRequestException;
import com.aws.greengrass.mqttclient.spool.SpoolerStoreException;
import com.aws.greengrass.mqttclient.v5.Publish;
import com.aws.greengrass.mqttclient.v5.QOS;
import com.aws.greengrass.mqttclient.v5.Subscribe;
import com.aws.greengrass.mqttclient.v5.Unsubscribe;
import com.aws.greengrass.util.RetryUtils;
import com.aws.greengrass.util.Utils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import software.amazon.awssdk.crt.mqtt.MqttClientConnectionEvents;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public class IoTCoreClient implements MessageClient<com.aws.greengrass.mqtt.bridge.model.MqttMessage> {
    private static final Logger LOGGER = LogManager.getLogger(IoTCoreClient.class);
    public static final String LOG_KEY_TOPIC = "topic";
    private final RetryUtils.RetryConfig subscribeRetryConfig =
            RetryUtils.RetryConfig.builder().initialRetryInterval(Duration.ofSeconds(1L))
                    .maxRetryInterval(Duration.ofSeconds(120L)).maxAttempt(Integer.MAX_VALUE)
                    .retryableExceptions(Arrays.asList(ExecutionException.class, TimeoutException.class)).build();

    // TODO configurable?
    private static final long MQTT_OPERATION_TIMEOUT_MS = Duration.ofMinutes(1).toMillis();

    @Getter(AccessLevel.PROTECTED)
    private Set<String> subscribedIotCoreTopics = ConcurrentHashMap.newKeySet();
    @Getter(AccessLevel.PROTECTED)
    private Set<String> toSubscribeIotCoreTopics = new HashSet<>();
    private volatile Consumer<com.aws.greengrass.mqtt.bridge.model.MqttMessage> messageHandler;
    private Future<?> subscribeFuture;
    private final Object subscribeLock = new Object();
    @Getter // for testing
    private final MqttClient iotMqttClient;
    private final ExecutorService executorService;
    private final Map<String, Mqtt5RouteOptions> optionsByTopic;

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
            stopSubscribing();
        }

        @Override
        public void onConnectionResumed(boolean sessionPresent) {
            synchronized (subscribeLock)  {
                stopSubscribing();
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
     * @param iotMqttClient   for interacting with IoT Core
     * @param executorService for tasks asynchronously
     * @param optionsByTopic  mqtt5 options by topic
     */
    public IoTCoreClient(MqttClient iotMqttClient,
                         ExecutorService executorService,
                         Map<String, Mqtt5RouteOptions> optionsByTopic) {
        this.iotMqttClient = iotMqttClient;
        this.executorService = executorService;
        this.optionsByTopic = optionsByTopic;
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
        stopSubscribing();
        removeMappingAndSubscriptions();
    }

    private void removeMappingAndSubscriptions() {
        unsubscribeAll();
        subscribedIotCoreTopics.clear();
    }

    private void unsubscribeAll() {
        LOGGER.atDebug().kv("mapping", subscribedIotCoreTopics).log("Unsubscribe from IoT Core topics");
        for (String topic : subscribedIotCoreTopics) {
            try {
                unsubscribeFromIotCore(topic);
                LOGGER.atDebug().kv(LOG_KEY_TOPIC, topic).log("Unsubscribed from topic");
            } catch (ExecutionException | TimeoutException | MqttRequestException e) {
                LOGGER.atWarn().kv(LOG_KEY_TOPIC, topic).setCause(e).log("Unable to unsubscribe");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void unsubscribeFromIotCore(String topic)
            throws InterruptedException, ExecutionException, TimeoutException, MqttRequestException {
        Unsubscribe unsubscribeRequest = Unsubscribe.builder().topic(topic).subscriptionCallback(iotCoreCallback)
                .build();
        iotMqttClient.unsubscribe(unsubscribeRequest).get(MQTT_OPERATION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void publish(com.aws.greengrass.mqtt.bridge.model.MqttMessage message) throws MessageClientException {
        try {
            iotMqttClient.publish(Publish.builder()
                    .topic(message.getTopic())
                    .payload(message.getPayload())
                    .qos(QOS.AT_LEAST_ONCE)
                    .retain(message.isRetain())
                    .contentType(message.getContentType())
                    .correlationData(message.getCorrelationData())
                    .responseTopic(message.getResponseTopic())
                    .messageExpiryIntervalSeconds(message.getMessageExpiryIntervalSeconds())
                    .payloadFormat(message.getPayloadFormat())
                    .userProperties(message.getUserProperties())
                    .build());
        } catch (MqttRequestException | SpoolerStoreException e) {
            throw new MessageClientException("Unable to publish message", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void updateSubscriptions(Set<String> topics,
                                    @NonNull Consumer<com.aws.greengrass.mqtt.bridge.model.MqttMessage>
                                            messageHandler) {
        synchronized (subscribeLock) {
            if (subscribeFuture != null) {
                subscribeFuture.cancel(true);
            }

            this.messageHandler = messageHandler;
            this.toSubscribeIotCoreTopics = new HashSet<>(topics);
            LOGGER.atDebug().kv("topics", topics).log("Subscribing to IoT Core topics");

            Set<String> topicsToRemove = new HashSet<>(subscribedIotCoreTopics);
            topicsToRemove.removeAll(topics);

            for (String topic : topicsToRemove) {
                try {
                    unsubscribeFromIotCore(topic);
                    LOGGER.atDebug().kv(LOG_KEY_TOPIC, topic).log("Unsubscribed from topic");
                    subscribedIotCoreTopics.remove(topic);
                } catch (MqttRequestException | ExecutionException | TimeoutException e) {
                    LOGGER.atError().kv(LOG_KEY_TOPIC, topic).setCause(e).log("Unable to unsubscribe");
                    // If we are unable to unsubscribe, leave the topic in the set
                    // so that we can try to remove next time.
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }

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
        for (String topic : topics) {
            try {
                RetryUtils.runWithRetry(subscribeRetryConfig, () -> {
                    try {
                        // retry only if client is connected; skip if offline.
                        // topics left here should be subscribed when the client
                        // is back online (onConnectionResumed event)
                        if (iotMqttClient.getMqttOnline().get()) {
                            subscribeToIotCore(topic);
                            subscribedIotCoreTopics.add(topic);
                        }
                        // useless return
                        return null;
                    } catch (ExecutionException e) {
                        Throwable cause = Utils.getUltimateCause(e);
                        if (cause instanceof InterruptedException) {
                            throw (InterruptedException) cause;
                        }
                        throw e;
                    }
                }, "subscribe-iotcore-topic", LOGGER);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                LOGGER.atError().kv(LOG_KEY_TOPIC, topic).setCause(e).log("Failed to subscribe to IoTCore topic");
            }
        }
    }

    private void subscribeToIotCore(String topic)
            throws InterruptedException, ExecutionException, MqttRequestException, TimeoutException {
        Subscribe.SubscribeBuilder builder = Subscribe.builder()
                .topic(topic)
                .callback(iotCoreCallback)
                .qos(QOS.AT_LEAST_ONCE);

        Optional.ofNullable(optionsByTopic.get(topic))
                .map(Mqtt5RouteOptions::isNoLocal)
                .ifPresent(builder::noLocal);

        iotMqttClient.subscribe(builder.build()).get(MQTT_OPERATION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    private void stopSubscribing() {
        synchronized (subscribeLock) {
            if (subscribeFuture != null) {
                subscribeFuture.cancel(true);
            }
        }
    }

    @Override
    public com.aws.greengrass.mqtt.bridge.model.MqttMessage convertMessage(Message message) {
        return (com.aws.greengrass.mqtt.bridge.model.MqttMessage) message.toMqtt();
    }
}
