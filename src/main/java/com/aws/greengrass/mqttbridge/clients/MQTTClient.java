/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge.clients;

import com.aws.greengrass.componentmanager.KernelConfigResolver;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqttbridge.Message;
import com.aws.greengrass.mqttbridge.auth.MQTTClientKeyStore;
import com.aws.greengrass.util.Coerce;
import com.aws.greengrass.util.Utils;
import lombok.AccessLevel;
import lombok.Getter;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.security.KeyStoreException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import javax.inject.Inject;
import javax.net.ssl.SSLSocketFactory;

public class MQTTClient implements MessageClient {
    private static final Logger LOGGER = LogManager.getLogger(MQTTClient.class);
    private static final String DEFAULT_BROKER_URI = "ssl://localhost:8883";
    private static final String DEFAULT_CLIENT_ID = "mqtt-bridge-" + Utils.generateRandomString(11);
    public static final String BROKER_URI_KEY = "brokerUri";
    public static final String CLIENT_ID_KEY = "clientId";
    public static final String TOPIC = "topic";
    private static final int MIN_WAIT_RETRY_IN_SECONDS = 1;
    private static final int MAX_WAIT_RETRY_IN_SECONDS = 120;

    private final MqttConnectOptions connOpts = new MqttConnectOptions();
    private Consumer<Message> messageHandler;
    private final String serverUri;
    private final String clientId;

    private final MqttClientPersistence dataStore;
    private final ExecutorService executorService;
    private Future<?> connectFuture;
    private IMqttClient mqttClientInternal;
    @Getter(AccessLevel.PROTECTED)
    private Set<String> subscribedLocalMqttTopics = new HashSet<>();
    private Set<String> toSubscribeLocalMqttTopics = new HashSet<>();

    private final MQTTClientKeyStore mqttClientKeyStore;

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
     * Ctr for MQTTClient.
     *
     * @param topics             topics passed in by Nucleus
     * @param mqttClientKeyStore KeyStore for MQTT Client
     * @param executorService    Executor service
     * @throws MQTTClientException if unable to create client for the mqtt broker
     */
    @Inject
    public MQTTClient(Topics topics, MQTTClientKeyStore mqttClientKeyStore, ExecutorService executorService)
            throws MQTTClientException {
        this(topics, mqttClientKeyStore, executorService, null);
        // TODO: Handle the case when serverUri is modified
        try {
            this.mqttClientInternal = new MqttClient(serverUri, clientId, dataStore);
        } catch (MqttException e) {
            throw new MQTTClientException("Unable to create an MQTT client", e);
        }
    }

    protected MQTTClient(Topics topics, MQTTClientKeyStore mqttClientKeyStore, ExecutorService executorService,
                         IMqttClient mqttClient) {
        this.mqttClientInternal = mqttClient;
        this.dataStore = new MemoryPersistence();
        this.serverUri = Coerce.toString(
                topics.findOrDefault(DEFAULT_BROKER_URI, KernelConfigResolver.CONFIGURATION_CONFIG_KEY,
                        BROKER_URI_KEY));
        this.clientId = Coerce.toString(
                topics.findOrDefault(DEFAULT_CLIENT_ID, KernelConfigResolver.CONFIGURATION_CONFIG_KEY, CLIENT_ID_KEY));
        this.mqttClientKeyStore = mqttClientKeyStore;
        this.mqttClientKeyStore.listenToUpdates(this::reset);
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

        if (serverUri.startsWith("ssl")) {
            SSLSocketFactory ssf = mqttClientKeyStore.getSSLSocketFactory();
            connOpts.setSocketFactory(ssf);
        }

        LOGGER.atInfo().kv("uri", serverUri).kv(CLIENT_ID_KEY, clientId).log("Connecting to broker");
        connectFuture = executorService.submit(this::reconnectAndResubscribe);
    }

    private synchronized void doConnect() throws MqttException {
        if (!mqttClientInternal.isConnected()) {
            mqttClientInternal.connect(connOpts);
            LOGGER.atInfo().kv("uri", serverUri).kv(CLIENT_ID_KEY, clientId).log("Connected to broker");
        }
    }

    private void reconnectAndResubscribe() {
        int waitBeforeRetry = MIN_WAIT_RETRY_IN_SECONDS;

        while (!mqttClientInternal.isConnected()) {
            try {
                // TODO: Clean up this loop
                doConnect();
            } catch (MqttException e) {
                LOGGER.atDebug().setCause(e)
                        .log("Unable to connect. Will be retried after {} seconds", waitBeforeRetry);
                try {
                    Thread.sleep(waitBeforeRetry * 1000);
                } catch (InterruptedException er) {
                    LOGGER.atError().setCause(er).log("Failed to reconnect");
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
