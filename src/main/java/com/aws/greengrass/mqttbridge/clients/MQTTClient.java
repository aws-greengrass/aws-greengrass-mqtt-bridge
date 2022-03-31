/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge.clients;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqttbridge.BridgeConfig;
import com.aws.greengrass.mqttbridge.Message;
import com.aws.greengrass.mqttbridge.auth.MQTTClientKeyStore;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import javax.net.ssl.SSLSocketFactory;

public class MQTTClient implements MessageClient {
    private static final Logger LOGGER = LogManager.getLogger(MQTTClient.class);

    public static final String ERROR_MISSING_USERNAME = "Password provided without username";

    public static final String TOPIC = "topic";
    private static final int MIN_WAIT_RETRY_IN_SECONDS = 1;
    private static final int MAX_WAIT_RETRY_IN_SECONDS = 120;

    private final MqttConnectOptions connOpts = new MqttConnectOptions();
    private final MQTTClientKeyStore.UpdateListener updateListener = this::reset;
    private Consumer<Message> messageHandler;
    private final Config config;

    private final MqttClientPersistence dataStore;
    private Future<?> connectFuture;
    private IMqttClient mqttClientInternal;
    @Getter(AccessLevel.PROTECTED)
    private Set<String> subscribedLocalMqttTopics = new HashSet<>();
    private Set<String> toSubscribeLocalMqttTopics = new HashSet<>();

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

    @Value
    @Builder
    public static class Config {
        @NonNull
        URI brokerUri;
        @NonNull
        String clientId;
        @NonNull
        String username;
        @NonNull
        String password;
        MQTTClientKeyStore mqttClientKeyStore;
        @NonNull
        ExecutorService executorService;
        IMqttClient internalMqttClientOverride;
    }

    /**
     * Construct an MQTTClient.
     *
     * @param config MQTTClient configuration
     * @throws MQTTClientException      if unable to create client for the mqtt broker
     */
    public MQTTClient(Config config) throws MQTTClientException {
        if (config.getUsername().isEmpty() && !config.getPassword().isEmpty()) {
            throw new MQTTClientException(ERROR_MISSING_USERNAME);
        }

        this.config = config;
        this.dataStore = new MemoryPersistence();

        if (this.config.getInternalMqttClientOverride() == null) {
            try {
                this.mqttClientInternal = new MqttClient(
                        this.config.getBrokerUri().toString(),
                        this.config.getClientId(),
                        this.dataStore
                );
            } catch (MqttException e) {
                throw new MQTTClientException("Unable to create an MQTT client", e);
            }
        } else {
            this.mqttClientInternal = this.config.getInternalMqttClientOverride();
        }

        this.config.getMqttClientKeyStore().listenToUpdates(updateListener);
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
     * @throws RuntimeException    if the client cannot load the KeyStore used to connect to the broker.
     * @throws MQTTClientException if client is already closed
     */
    public void start() throws MQTTClientException {
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
    public void stop() {
        config.getMqttClientKeyStore().unsubscribeToUpdates(updateListener);
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
    public synchronized void updateSubscriptions(Set<String> topics, Consumer<Message> messageHandler) {
        this.messageHandler = messageHandler;

        this.toSubscribeLocalMqttTopics = new HashSet<>(topics);
        LOGGER.atDebug().kv("topics", topics).log("Updated local MQTT topics to subscribe");

        if (mqttClientInternal.isConnected()) {
            updateSubscriptionsInternal();
        }
    }

    private synchronized void updateSubscriptionsInternal() {
        Set<String> topicsToRemove = new HashSet<>(subscribedLocalMqttTopics);
        topicsToRemove.removeAll(toSubscribeLocalMqttTopics);

        topicsToRemove.forEach(s -> {
            try {
                mqttClientInternal.unsubscribe(s);
                LOGGER.atDebug().kv(TOPIC, s).log("Unsubscribed from topic");
                subscribedLocalMqttTopics.remove(s);
            } catch (MqttException e) {
                LOGGER.atError().kv(TOPIC, s).setCause(e).log("Unable to unsubscribe");
                // If we are unable to unsubscribe, leave the topic in the set so that we can try to remove next time.
            }
        });

        Set<String> topicsToSubscribe = new HashSet<>(toSubscribeLocalMqttTopics);
        topicsToSubscribe.removeAll(subscribedLocalMqttTopics);

        // TODO: Support configurable qos, add retry
        topicsToSubscribe.forEach(s -> {
            try {
                mqttClientInternal.subscribe(s);
                LOGGER.atDebug().kv(TOPIC, s).log("Subscribed to topic");
                subscribedLocalMqttTopics.add(s);
            } catch (MqttException e) {
                LOGGER.atError().kv(TOPIC, s).log("Failed to subscribe");
            }
        });
    }

    private synchronized void connectAndSubscribe() throws KeyStoreException {
        if (connectFuture != null) {
            connectFuture.cancel(true);
        }

        //TODO: persistent session could be used
        connOpts.setCleanSession(true);

        if ("ssl".equalsIgnoreCase(config.getBrokerUri().getScheme())) {
            SSLSocketFactory ssf = config.getMqttClientKeyStore().getSSLSocketFactory();
            connOpts.setSocketFactory(ssf);
        }

        if (!config.getUsername().isEmpty()) {
            connOpts.setUserName(config.getUsername());
            if (!config.getPassword().isEmpty()) {
                connOpts.setPassword(config.getPassword().toCharArray());
            }
        }

        LOGGER.atInfo().kv("uri", config.getBrokerUri()).kv(BridgeConfig.KEY_CLIENT_ID, config.getClientId())
                .log("Connecting to broker");
        connectFuture = config.getExecutorService().submit(this::reconnectAndResubscribe);
    }

    private synchronized void doConnect() throws MqttException {
        if (!mqttClientInternal.isConnected()) {
            mqttClientInternal.connect(connOpts);
            LOGGER.atInfo().kv("uri", config.getBrokerUri()).kv(BridgeConfig.KEY_CLIENT_ID, config.getClientId())
                    .log("Connected to broker");
        }
    }

    private void reconnectAndResubscribe() {
        int waitBeforeRetry = MIN_WAIT_RETRY_IN_SECONDS;

        while (!mqttClientInternal.isConnected()) {
            // prevent connection attempt when interrupted
            if (Thread.interrupted()) {
                return;
            }

            try {
                // TODO: Clean up this loop
                doConnect();
            } catch (MqttException e) {
                LOGGER.atDebug().setCause(e)
                        .log("Unable to connect. Will be retried after {} seconds", waitBeforeRetry);
                try {
                    Thread.sleep(waitBeforeRetry * 1000);
                } catch (InterruptedException er) {
                    Thread.currentThread().interrupt();
                    LOGGER.atInfo().setCause(er).log("Aborting connection due to interrupt");
                    return;
                }
                waitBeforeRetry = Math.min(2 * waitBeforeRetry, MAX_WAIT_RETRY_IN_SECONDS);
            }
        }

        resubscribe();
    }

    private synchronized void resubscribe() {
        subscribedLocalMqttTopics.clear();
        // Resubscribe to topics
        updateSubscriptionsInternal();
    }
}
