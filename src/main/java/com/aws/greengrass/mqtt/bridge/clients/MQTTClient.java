/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.clients;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqtt.bridge.BridgeConfig;
import com.aws.greengrass.mqtt.bridge.auth.MQTTClientKeyStore;
import com.aws.greengrass.mqtt.bridge.model.Message;
import com.aws.greengrass.mqtt.bridge.model.MqttMessage;
import com.aws.greengrass.util.CrashableSupplier;
import com.aws.greengrass.util.RetryUtils;
import com.aws.greengrass.util.Utils;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.net.URI;
import java.security.KeyStoreException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import javax.net.ssl.SSLSocketFactory;

@SuppressWarnings("PMD.CloseResource")
public class MQTTClient implements MessageClient<MqttMessage>, Configurable {
    private static final Logger LOGGER = LogManager.getLogger(MQTTClient.class);

    public static final String TOPIC = "topic";
    private static final int MIN_WAIT_RETRY_IN_SECONDS = 1;
    private static final int MAX_WAIT_RETRY_IN_SECONDS = 120;

    private Consumer<MqttMessage> messageHandler;
    private final MqttClientPersistence dataStore;
    private final ExecutorService executorService;
    private final Object subscribeLock = new Object();
    private final Object connectTaskLock = new Object();
    private Future<?> connectFuture;
    private Future<?> subscribeFuture;
    @Getter // for testing
    private volatile IMqttClient mqttClientInternal;
    private final CrashableSupplier<IMqttClient, MqttException> clientFactory;
    @Getter(AccessLevel.PROTECTED)
    private Set<String> subscribedLocalMqttTopics = ConcurrentHashMap.newKeySet();
    private Set<String> toSubscribeLocalMqttTopics = new HashSet<>();

    private final MQTTClientKeyStore.UpdateListener onKeyStoreUpdate = new MQTTClientKeyStore.UpdateListener() {
        @Override
        public void onCAUpdate() {
            if (!started()) {
                LOGGER.atDebug().log("Client not yet initialized, skipping reset");
                return;
            }
            LOGGER.atInfo().log("New CA certificate available, reconnecting client");
            reset(false);
        }

        @Override
        public void onClientCertUpdate() {
            LOGGER.atInfo().log("New client certificate available and will be used "
                    + "the next time this client reconnects");
        }
    };

    private final MQTTClientKeyStore mqttClientKeyStore;

    private final RetryUtils.RetryConfig mqttExceptionRetryConfig =
            RetryUtils.RetryConfig.builder().initialRetryInterval(Duration.ofSeconds(1L))
                    .maxRetryInterval(Duration.ofSeconds(120L)).maxAttempt(Integer.MAX_VALUE)
                    .retryableExceptions(Collections.singletonList(MqttException.class)).build();

    @Getter // for testing
    @Setter
    private MqttCallback mqttCallback = new MqttCallback() {
        @Override
        public void connectionLost(Throwable cause) {
            LOGGER.atDebug().setCause(cause).log("MQTT client disconnected, reconnecting...");
            reconnectAndResubscribeAsync();
        }

        @Override
        public void messageArrived(String topic, org.eclipse.paho.client.mqttv3.MqttMessage message) {
            LOGGER.atTrace().kv(TOPIC, topic).log("Received MQTT message");

            if (messageHandler == null) {
                LOGGER.atWarn().kv(TOPIC, topic).log("MQTT message received but message handler not set");
            } else {
                messageHandler.accept(MqttMessage.fromPahoMQTT3(topic, message));
            }
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {
        }
    };

    @Getter // for testing
    volatile Config config;
    volatile Config pendingConfig;

    @Data
    @Builder(toBuilder = true)
    public static class Config {
        URI brokerUri;
        String clientId;

        /**
         * Map from bridge configuration to client configuration.
         *
         * @param bridgeConfig component configuration
         * @return client configuration
         */
        public static Config fromBridgeConfig(BridgeConfig bridgeConfig) {
            return Config.builder()
                    .brokerUri(bridgeConfig.getBrokerUri())
                    .clientId(bridgeConfig.getClientId())
                    .build();
        }
    }

    /**
     * Apply new configuration to this client. Client will reset.
     *
     * @param bridgeConfig new bridge configuration
     */
    @Override
    public void applyConfig(@NonNull BridgeConfig bridgeConfig) {
        applyConfig(Config.fromBridgeConfig(bridgeConfig));
    }

    void applyConfig(Config newConfig) {
        if (Objects.equals(this.config, newConfig)) {
            return;
        }
        // config will be picked up the next time an mqtt client is created
        this.pendingConfig = newConfig;
        if (!started()) {
            return;
        }
        reset(true);
    }

    private boolean started() {
        return mqttClientInternal != null;
    }

    /**
     * Construct an MQTTClient.
     *
     * @param bridgeConfig       bridge config
     * @param mqttClientKeyStore KeyStore for MQTT Client
     * @param executorService    Executor service
     */
    public MQTTClient(@NonNull BridgeConfig bridgeConfig,
                      MQTTClientKeyStore mqttClientKeyStore,
                      ExecutorService executorService) {
        this.config = Config.fromBridgeConfig(bridgeConfig);
        this.dataStore = new MemoryPersistence();
        this.clientFactory = () -> {
            if (this.pendingConfig != null) {
                this.config = this.pendingConfig;
            }
            MqttClient client = new MqttClient(config.getBrokerUri().toString(), config.getClientId(), dataStore);
            client.setCallback(mqttCallback);
            return client;
        };
        this.mqttClientKeyStore = mqttClientKeyStore;
        this.mqttClientKeyStore.listenToUpdates(onKeyStoreUpdate);
        this.executorService = executorService;
    }

    protected MQTTClient(Config config,
                         MQTTClientKeyStore mqttClientKeyStore,
                         ExecutorService executorService,
                         CrashableSupplier<IMqttClient, MqttException> clientFactory) {
        this.config = config;
        this.clientFactory = () -> {
            if (this.pendingConfig != null) {
                this.config = this.pendingConfig;
            }
            IMqttClient client = clientFactory.apply();
            client.setCallback(mqttCallback);
            return client;
        };
        this.dataStore = new MemoryPersistence();
        this.mqttClientKeyStore = mqttClientKeyStore;
        this.mqttClientKeyStore.listenToUpdates(onKeyStoreUpdate);
        this.executorService = executorService;
    }

    synchronized void reset(boolean recreateClient) {
        disconnectForcibly();
        if (recreateClient) {
            try {
                this.mqttClientInternal = clientFactory.apply();
            } catch (MqttException e) {
                LOGGER.atError().cause(e).log("unable to recreate MQTT client using new configuration, "
                        + "falling back to old configuration");
            }
        }
        connectAndSubscribe();
    }

    /**
     * Start the {@link MQTTClient}.
     */
    @Override
    public void start() throws MessageClientException {
        try {
            this.mqttClientInternal = clientFactory.apply();
        } catch (MqttException e) {
            throw new MessageClientException("Unable to create MQTTClient", e);
        }
        connectAndSubscribe();
    }

    @SuppressWarnings({"PMD.AvoidCatchingGenericException", "PMD.AvoidCatchingNPE"})
    private void disconnectForcibly() {
        IMqttClient client = mqttClientInternal;
        if (client == null) {
            return;
        }
        try {
            long doNotQuiesce = 0L;
            long doNotWaitForDisconnectPacket = 1L; // since 0L means no timeout
            client.disconnectForcibly(doNotQuiesce, doNotWaitForDisconnectPacket);
        } catch (MqttException e) {
            LOGGER.atWarn().cause(e).log("Unable to disconnect forcibly");
        } catch (NullPointerException ignore) {
            // can happen if disconnectForcibly is called after the client is closed.
            // since we're already disconnected, this is not a real error
        }
    }

    /**
     * Stop the {@link MQTTClient}.
     */
    @Override
    @SuppressWarnings({"PMD.AvoidCatchingGenericException", "PMD.AvoidCatchingNPE"})
    public void stop() {
        mqttClientKeyStore.unsubscribeFromUpdates(onKeyStoreUpdate);

        IMqttClient client = mqttClientInternal;
        try {
            if (client != null) {
                // ensure that client cannot reconnect again
                // if callbacks would trigger for whatever reason
                client.setCallback(null);
            }
        } catch (NullPointerException ignore) {
            // can happen if client is closed.
            // ignoring as it is not a real error
        }

        cancelConnectTask();
        disconnectForcibly();

        try {
            dataStore.close();
        } catch (MqttPersistenceException e) {
            LOGGER.atDebug().setCause(e).log("Unable to close mqtt client datastore");
        }

        try {
            if (client != null) {
                client.close();
            }
        } catch (MqttException e) {
            LOGGER.atWarn().setCause(e).log("Unable to close MQTT client");
        }
    }

    @Override
    public void publish(MqttMessage message) throws MessageClientException {
        try {
            mqttClientInternal
                    .publish(message.getTopic(), new org.eclipse.paho.client.mqttv3.MqttMessage(message.getPayload()));
        } catch (MqttException e) {
            LOGGER.atError().setCause(e).kv(TOPIC, message.getTopic()).log("MQTT publish failed");
            throw new MQTTClientException("Failed to publish message", e);
        }
    }

    @Override
    public void updateSubscriptions(Set<String> topics, Consumer<MqttMessage> messageHandler) {
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
        connOpts.setMaxInflight(1000);
        if ("ssl".equalsIgnoreCase(config.getBrokerUri().getScheme())) {
            SSLSocketFactory ssf = mqttClientKeyStore.getSSLSocketFactory();
            connOpts.setSocketFactory(ssf);
        }
        return connOpts;
    }

    private void connectAndSubscribe() {
        LOGGER.atInfo()
                .kv(BridgeConfig.KEY_BROKER_URI, config.getBrokerUri())
                .kv(BridgeConfig.KEY_CLIENT_ID, config.getClientId())
                .log("Connecting to broker");
        reconnectAndResubscribeAsync();
    }

    private void reconnectAndResubscribeAsync() {
        synchronized (connectTaskLock) {
            cancelConnectTask();
            connectFuture = executorService.submit(this::reconnectAndResubscribe);
        }
    }

    private void cancelConnectTask() {
        synchronized (connectTaskLock) {
            if (connectFuture != null) {
                connectFuture.cancel(true);
            }
        }
    }

    private void reconnectAndResubscribe() {
        int waitBeforeRetry = MIN_WAIT_RETRY_IN_SECONDS;

        IMqttClient client = mqttClientInternal;
        while (!client.isConnected() && !Thread.currentThread().isInterrupted()) {
            Exception error;
            try {
                // TODO: Clean up this loop
                client.connect(getConnectionOptions());
                break;
            } catch (MqttException e) {
                if (Utils.getUltimateCause(e) instanceof InterruptedException) {
                    // paho doesn't reset the interrupt flag
                    LOGGER.atDebug().log("Interrupted during reconnect");
                    Thread.currentThread().interrupt();
                    return;
                }
                if (MqttException.REASON_CODE_CLIENT_CLOSED == e.getReasonCode()) {
                    return;
                }
                error = e;
            } catch (KeyStoreException e) {
                error = e;
            }

            LOGGER.atDebug().setCause(error)
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

        LOGGER.atInfo()
                .kv(BridgeConfig.KEY_BROKER_URI, config.getBrokerUri())
                .kv(BridgeConfig.KEY_CLIENT_ID, config.getClientId())
                .log("Connected to broker");

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

    @Override
    public MqttMessage convertMessage(Message message) {
        return (MqttMessage) message.toMqtt();
    }
}
