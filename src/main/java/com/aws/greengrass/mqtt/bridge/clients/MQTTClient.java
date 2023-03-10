/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.clients;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqtt.bridge.BridgeConfig;
import com.aws.greengrass.mqtt.bridge.Message;
import com.aws.greengrass.mqtt.bridge.auth.MQTTClientKeyStore;
import com.aws.greengrass.util.RetryUtils;
import com.aws.greengrass.util.Utils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.net.URI;
import java.security.KeyStoreException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import javax.net.ssl.SSLSocketFactory;

public class MQTTClient implements MessageClient {
    private static final Logger LOGGER = LogManager.getLogger(MQTTClient.class);

    public static final String TOPIC = "topic";
    private static final int MIN_WAIT_RETRY_IN_SECONDS = 1;
    private static final int MAX_WAIT_RETRY_IN_SECONDS = 120;

    private Consumer<Message> messageHandler;
    private final URI brokerUri;
    private final String clientId;

    private final MqttClientPersistence dataStore;
    private final ExecutorService executorService;
    private final Object subscribeLock = new Object();
    private Future<?> connectFuture;
    private Future<?> subscribeFuture;
    private IMqttClient mqttClientInternal;
    @Getter(AccessLevel.PROTECTED)
    private Set<String> subscribedLocalMqttTopics = ConcurrentHashMap.newKeySet();
    private Set<String> toSubscribeLocalMqttTopics = new HashSet<>();

    private final MQTTClientKeyStore mqttClientKeyStore;

    private final RetryUtils.RetryConfig mqttExceptionRetryConfig =
            RetryUtils.RetryConfig.builder().initialRetryInterval(Duration.ofSeconds(1L))
                    .maxRetryInterval(Duration.ofSeconds(120L)).maxAttempt(Integer.MAX_VALUE)
                    .retryableExceptions(Collections.singletonList(MqttException.class)).build();

    private final MqttCallback mqttCallback = new MqttCallback() {
        @Override
        public void connectionLost(Throwable cause) {
            LOGGER.atDebug().setCause(cause).log("MQTT client disconnected, reconnecting...");
            reconnectAndResubscribe();
        }

        @Override
        public void messageArrived(String topic, org.eclipse.paho.client.mqttv3.MqttMessage message) {
            LOGGER.atTrace().kv(TOPIC, topic).log("Received MQTT message");

            if (messageHandler == null) {
                LOGGER.atWarn().kv(TOPIC, topic).log("MQTT message received but message handler not set");
            } else {
                Message msg = new Message(topic, message.getPayload());
                messageHandler.accept(msg);
            }
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {
        }
    };

    /**
     * Construct an MQTTClient.
     *
     * @param brokerUri          broker uri
     * @param clientId           client id
     * @param mqttClientKeyStore KeyStore for MQTT Client
     * @param executorService    Executor service
     * @throws MQTTClientException if unable to create client for the mqtt broker
     */
    public MQTTClient(@NonNull URI brokerUri, @NonNull String clientId, MQTTClientKeyStore mqttClientKeyStore,
                      ExecutorService executorService) throws MQTTClientException {
        this(brokerUri, clientId, mqttClientKeyStore, executorService, null);
        // TODO: Handle the case when serverUri is modified
        try {
            this.mqttClientInternal = new MqttClient(brokerUri.toString(), clientId, dataStore);
        } catch (MqttException e) {
            throw new MQTTClientException("Unable to create an MQTT client", e);
        }
    }

    protected MQTTClient(@NonNull URI brokerUri, @NonNull String clientId, MQTTClientKeyStore mqttClientKeyStore,
                         ExecutorService executorService, IMqttClient mqttClient) {
        this.brokerUri = brokerUri;
        this.clientId = clientId;
        this.mqttClientInternal = mqttClient;
        this.dataStore = new MemoryPersistence();
        this.mqttClientKeyStore = mqttClientKeyStore;
        this.mqttClientKeyStore.listenToCAUpdates(this::reset);
        this.executorService = executorService;
    }

    void reset() {
        if (mqttClientInternal.isConnected()) {
            try {
                mqttClientInternal.disconnect();
            } catch (MqttException e) {
                LOGGER.atError().setCause(e).log("Failed to disconnect MQTT client");
                return;
            }
        }

        try {
            connectAndSubscribe();
        } catch (KeyStoreException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Start the {@link MQTTClient}.
     *
     * @throws RuntimeException if the client cannot load the KeyStore used to connect to the broker.
     */
    @Override
    public void start() {
        mqttClientInternal.setCallback(mqttCallback);
        try {
            connectAndSubscribe();
        } catch (KeyStoreException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Stop the {@link MQTTClient}.
     */
    @Override
    public void stop() {
        removeMappingAndSubscriptions();

        try {
            if (mqttClientInternal.isConnected()) {
                mqttClientInternal.disconnect();
            }
            dataStore.close();
        } catch (MqttException e) {
            LOGGER.atError().setCause(e).log("Failed to disconnect MQTT client");
        }
    }

    private synchronized void removeMappingAndSubscriptions() {
        unsubscribeAll();
        subscribedLocalMqttTopics.clear();
    }

    private void unsubscribeAll() {
        LOGGER.atDebug().kv("mapping", subscribedLocalMqttTopics).log("Unsubscribe from local MQTT topics");

        this.subscribedLocalMqttTopics.forEach(s -> {
            try {
                mqttClientInternal.unsubscribe(s);
                LOGGER.atDebug().kv(TOPIC, s).log("Unsubscribed from topic");
            } catch (MqttException e) {
                LOGGER.atWarn().kv(TOPIC, s).setCause(e).log("Unable to unsubscribe");
            }
        });
    }

    @Override
    public void publish(Message message) throws MessageClientException {
        try {
            mqttClientInternal
                    .publish(message.getTopic(), new org.eclipse.paho.client.mqttv3.MqttMessage(message.getPayload()));
        } catch (MqttException e) {
            LOGGER.atError().setCause(e).kv(TOPIC, message.getTopic()).log("MQTT publish failed");
            throw new MQTTClientException("Failed to publish message", e);
        }
    }

    @Override
    public boolean supportsTopicFilters() {
        return true;
    }

    @Override
    public void updateSubscriptions(Set<String> topics, Consumer<Message> messageHandler) {
        this.messageHandler = messageHandler;

        this.toSubscribeLocalMqttTopics = new HashSet<>(topics);
        LOGGER.atDebug().kv("topics", topics).log("Updated local MQTT topics to subscribe");

        if (mqttClientInternal.isConnected()) {
            updateSubscriptionsInternal();
        }
    }

    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    private void updateSubscriptionsInternal() {
        synchronized (subscribeLock) {
            if (subscribeFuture != null) {
                subscribeFuture.cancel(true);
            }
            Set<String> topicsToRemove = new HashSet<>(subscribedLocalMqttTopics);
            topicsToRemove.removeAll(toSubscribeLocalMqttTopics);

            topicsToRemove.forEach(s -> {
                try {
                    mqttClientInternal.unsubscribe(s);
                    LOGGER.atDebug().kv(TOPIC, s).log("Unsubscribed from topic");
                    subscribedLocalMqttTopics.remove(s);
                } catch (MqttException e) {
                    LOGGER.atError().kv(TOPIC, s).setCause(e).log("Unable to unsubscribe");
                    // If we are unable to unsubscribe, leave the topic in the set
                    // so that we can try to remove next time.
                }
            });

            Set<String> topicsToSubscribe = new HashSet<>(toSubscribeLocalMqttTopics);
            topicsToSubscribe.removeAll(subscribedLocalMqttTopics);

            LOGGER.atDebug().kv("topics", topicsToSubscribe).log("Subscribing to MQTT topics");

            subscribeFuture = executorService.submit(() -> subscribeToTopics(topicsToSubscribe));
        }
    }

    private MqttConnectOptions getConnectionOptions() throws KeyStoreException {
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);

        if ("ssl".equalsIgnoreCase(brokerUri.getScheme())) {
            SSLSocketFactory ssf = mqttClientKeyStore.getSSLSocketFactory();
            connOpts.setSocketFactory(ssf);
        }

        return connOpts;
    }

    private synchronized void connectAndSubscribe() throws KeyStoreException {
        if (connectFuture != null) {
            connectFuture.cancel(true);
        }

        LOGGER.atInfo()
                .kv(BridgeConfig.KEY_BROKER_URI, brokerUri)
                .kv(BridgeConfig.KEY_CLIENT_ID, clientId)
                .log("Connecting to broker");

        connectFuture = executorService.submit(this::reconnectAndResubscribe);
    }

    private synchronized void doConnect() throws MqttException, KeyStoreException {
        if (!mqttClientInternal.isConnected()) {
            mqttClientInternal.connect(getConnectionOptions());
            LOGGER.atInfo()
                    .kv(BridgeConfig.KEY_BROKER_URI, brokerUri)
                    .kv(BridgeConfig.KEY_CLIENT_ID, clientId)
                    .log("Connected to broker");
        }
    }

    private void reconnectAndResubscribe() {
        int waitBeforeRetry = MIN_WAIT_RETRY_IN_SECONDS;

        while (!mqttClientInternal.isConnected() && !Thread.currentThread().isInterrupted()) {
            try {
                // TODO: Clean up this loop
                doConnect();
            } catch (MqttException | KeyStoreException e) {
                if (Utils.getUltimateCause(e) instanceof InterruptedException) {
                    // paho doesn't reset the interrupt flag
                    LOGGER.atDebug().log("Interrupted during reconnect");
                    Thread.currentThread().interrupt();
                    return;
                }

                LOGGER.atDebug().setCause(e)
                        .log("Unable to connect. Will be retried after {} seconds", waitBeforeRetry);
                try {
                    Thread.sleep(waitBeforeRetry * 1000);
                } catch (InterruptedException er) {
                    Thread.currentThread().interrupt();
                    LOGGER.atDebug().log("Interrupted during reconnect");
                    return;
                }
                waitBeforeRetry = Math.min(2 * waitBeforeRetry, MAX_WAIT_RETRY_IN_SECONDS);
            }
        }

        resubscribe();
    }

    private void resubscribe() {
        subscribedLocalMqttTopics.clear();
        // Resubscribe to topics
        updateSubscriptionsInternal();
    }

    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    private void subscribeToTopics(Set<String> topics) {
        // TODO: Support configurable qos
        // retry until interrupted
        topics.forEach(s -> {
            try {
                RetryUtils.runWithRetry(mqttExceptionRetryConfig, () -> {
                    mqttClientInternal.subscribe(s);
                    // useless return
                    return null;
                }, "subscribe-mqtt-topic", LOGGER);
                subscribedLocalMqttTopics.add(s);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                LOGGER.atError().setCause(e).kv(TOPIC, s).log("Failed to subscribe");
            }
        });
    }
}
