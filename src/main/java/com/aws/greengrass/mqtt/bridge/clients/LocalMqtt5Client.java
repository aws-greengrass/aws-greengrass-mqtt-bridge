package com.aws.greengrass.mqtt.bridge.clients;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqtt.bridge.BridgeConfig;
import com.aws.greengrass.mqtt.bridge.model.Message;
import com.aws.greengrass.mqtt.bridge.model.MqttMessage;
import com.aws.greengrass.util.RetryUtils;
import com.aws.greengrass.util.Utils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import org.eclipse.paho.client.mqttv3.MqttException;
import software.amazon.awssdk.crt.CrtRuntimeException;
import software.amazon.awssdk.crt.mqtt5.Mqtt5Client;
import software.amazon.awssdk.crt.mqtt5.Mqtt5ClientOptions;
import software.amazon.awssdk.crt.mqtt5.OnAttemptingConnectReturn;
import software.amazon.awssdk.crt.mqtt5.OnConnectionFailureReturn;
import software.amazon.awssdk.crt.mqtt5.OnConnectionSuccessReturn;
import software.amazon.awssdk.crt.mqtt5.OnDisconnectionReturn;
import software.amazon.awssdk.crt.mqtt5.OnStoppedReturn;
import software.amazon.awssdk.crt.mqtt5.PublishResult;
import software.amazon.awssdk.crt.mqtt5.QOS;
import software.amazon.awssdk.crt.mqtt5.packets.PublishPacket;
import software.amazon.awssdk.crt.mqtt5.packets.SubAckPacket;
import software.amazon.awssdk.crt.mqtt5.packets.SubscribePacket;
import software.amazon.awssdk.crt.mqtt5.packets.UnsubAckPacket;
import software.amazon.awssdk.crt.mqtt5.packets.UnsubscribePacket;
import com.aws.greengrass.mqtt.bridge.auth.MQTTClientKeyStore;

import java.net.URI;
import java.security.KeyStoreException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class LocalMqtt5Client implements MessageClient<MqttMessage> {
    private static final Logger LOGGER = LogManager.getLogger(LocalMqtt5Client.class);
    public static final String TOPIC = "topic";
    private static final int MIN_WAIT_RETRY_IN_SECONDS = 1;
    private static final int MAX_WAIT_RETRY_IN_SECONDS = 120;
    private static final QOS QOS = software.amazon.awssdk.crt.mqtt5.QOS.AT_LEAST_ONCE;
    private final URI brokerUri;
    private final String clientId;
    private Mqtt5Client client;
    private Consumer<MqttMessage> messageHandler;
    private final MQTTClientKeyStore mqttClientKeyStore;
    private final ExecutorService executorService;

    @Getter(AccessLevel.PROTECTED)
    private Set<String> subscribedLocalMqttTopics = ConcurrentHashMap.newKeySet();
    private Set<String> toSubscribeLocalMqttTopics = new HashSet<>();

    private final RetryUtils.RetryConfig mqttExceptionRetryConfig =
            RetryUtils.RetryConfig.builder().initialRetryInterval(Duration.ofSeconds(1L))
                    .maxRetryInterval(Duration.ofSeconds(120L)).maxAttempt(Integer.MAX_VALUE)
                    .retryableExceptions(Collections.singletonList(MqttException.class)).build();

    private final Object subscribeLock = new Object();
    CompletableFuture<SubAckPacket> subscribeFuture;

    private final AtomicReference<CompletableFuture<Void>> stopFuture = new AtomicReference<>(null);
    @Getter(AccessLevel.PACKAGE)
    private final Mqtt5ClientOptions.LifecycleEvents connectionEventCallback =
            new Mqtt5ClientOptions.LifecycleEvents() {
        @Override
        public void onAttemptingConnect(Mqtt5Client client,
                                       OnAttemptingConnectReturn onAttemptingConnectReturn) {
            LOGGER.atDebug().log("Attempting to connect to AWS IoT Core");
        }

        @Override
        public void onConnectionSuccess(Mqtt5Client client, OnConnectionSuccessReturn onConnectionSuccessReturn) {

        }

        @Override
        public void onConnectionFailure(Mqtt5Client client, OnConnectionFailureReturn onConnectionFailureReturn) {

        }

        @Override
        public void onDisconnection(Mqtt5Client client, OnDisconnectionReturn onDisconnectionReturn) {

        }

        @Override
        public void onStopped(Mqtt5Client client, OnStoppedReturn onStoppedReturn) {
            client.close();
            CompletableFuture<Void> f = stopFuture.get();
            if (f != null) {
                f.complete(null);
            }
        }
    };

    /**
     * TODO: Implement constructor
     */
    public LocalMqtt5Client(@NonNull URI brokerUri, @NonNull String clientId, MQTTClientKeyStore mqttClientKeyStore,
                             ExecutorService executorService, Mqtt5ClientOptions mqtt5ClientOptions) {
        this.brokerUri = brokerUri;
        this.clientId = clientId;
        this.mqttClientKeyStore = mqttClientKeyStore;
        this.mqttClientKeyStore.listenToCAUpdates(this::reset);
        this.executorService = executorService;
        this.client = new Mqtt5Client(mqtt5ClientOptions);
    }

    void reset() {
        if (client.getIsConnected()) {
            // TODO: Is it okay to removeMappingAndSubscriptions here?
            stop();
        }

        try {
            connectAndSubscribe();
        } catch (CrtRuntimeException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void publish(MqttMessage message) throws MessageClientException {
        if(client.getIsConnected()) {

            PublishPacket publishPacket = new PublishPacket.PublishPacketBuilder()
                    .withPayload(message.getPayload())
                    .withQOS(QOS)
                    .withTopic(message.getTopic())
                    .build();
            LOGGER.atDebug().kv("topic", message.getTopic()).kv("message", message).log("Publishing message to MQTT "
                    + "topic");
            //TODO: Figure out what to do with publishFuture
            CompletableFuture<PublishResult> publishFuture = client.publish(publishPacket);
        }
    }
    @Override
    public boolean supportsTopicFilters() {
        return true;
    }

    @Override
    public void updateSubscriptions(Set<String> topics, Consumer<MqttMessage> messageHandler) {
        // TODO: private Mqtt5ClientOptions.PublishEvents messageHandler;
        this.messageHandler = messageHandler;

        this.toSubscribeLocalMqttTopics = new HashSet<>(topics);
        LOGGER.atDebug().kv("topics", topics).log("Updated local MQTT5 topics to subscribe");
        if (client.getIsConnected()) {
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
                    //TODO: Handle unsubscribe Future failure and success
                    unsubscribe(s);
                    // LOGGER.atDebug().kv(TOPIC, s).log("Unsubscribed from topic");
                    subscribedLocalMqttTopics.remove(s);
                } catch (Exception e) {
                    LOGGER.atError().kv(TOPIC, s).setCause(e).log("Unable to unsubscribe");
                    // If we are unable to unsubscribe, leave the topic in the set
                    // so that we can try to remove next time.
                }
            });

            Set<String> topicsToSubscribe = new HashSet<>(toSubscribeLocalMqttTopics);
            topicsToSubscribe.removeAll(subscribedLocalMqttTopics);

            LOGGER.atDebug().kv("topics", topicsToSubscribe).log("Subscribing to MQTT topics");


            //subscribeFuture = executorService.submit(() -> subscribeToTopics(topicsToSubscribe));
            //subscribeFuture = CompletableFuture.runAsync(subscribeToTopics(topicsToSubscribe) ,executorService);
            subscribeToTopics(topicsToSubscribe);

        }
    }

    public void subscribe(String topic) {
        if (client.getIsConnected()) {
            SubscribePacket subscribePacket = new SubscribePacket.SubscribePacketBuilder()
                    .withSubscription(topic, QOS).build();
            LOGGER.atDebug().kv(TOPIC, topic).log("Subscribing to MQTT topic");
            //TODO: deal with subscribeFuture
            this.subscribeFuture = client.subscribe(subscribePacket);
        }
    }

    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    private void subscribeToTopics(Set<String> topics) {
        // TODO: Support configurable qos
        // retry until interrupted
        topics.forEach(s -> {
            try {
                RetryUtils.runWithRetry(mqttExceptionRetryConfig, () -> {
                    subscribe(s);
                    // useless return
                    return null;
                }, "subscribe-mqtt5-topic", LOGGER);
                subscribedLocalMqttTopics.add(s);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                LOGGER.atError().setCause(e).kv(TOPIC, s).log("Failed to subscribe");
            }
        });
    }

    private void resubscribe() {
        subscribedLocalMqttTopics.clear();
        // Resubscribe to topics
        updateSubscriptionsInternal();
    }

    private void unsubscribe(String topic) {
        if (client.getIsConnected()) {
            UnsubscribePacket unsubscribePacket =
                    new UnsubscribePacket.UnsubscribePacketBuilder().withSubscription(topic).build();
            LOGGER.atDebug().kv(TOPIC, topic).log("Unsubscribing from MQTT topic");
            //TODO: deal with unsubscribeFuture
            CompletableFuture<UnsubAckPacket> unsubscribeFuture = client.unsubscribe(unsubscribePacket);
        }
    }

    private synchronized void removeMappingAndSubscriptions() {
        unsubscribeAll();
        subscribedLocalMqttTopics.clear();
    }

    private void unsubscribeAll() {
        LOGGER.atDebug().kv("mapping", subscribedLocalMqttTopics).log("Unsubscribe from local MQTT topics");

        this.subscribedLocalMqttTopics.forEach(s -> {
            unsubscribe(s);
        });
    }

    @Override
    public void start() {
        // TODO: Set callback?
        try {
            // client.start();
            connectAndSubscribe();
        } catch (CrtRuntimeException e) {
            // throw new MessageClientException("Unable to start Local Mqtt 5 Client", e);
        }
    }

    private synchronized void connectAndSubscribe() throws CrtRuntimeException {
        LOGGER.atInfo()
                .kv(BridgeConfig.KEY_BROKER_URI, brokerUri)
                .kv(BridgeConfig.KEY_CLIENT_ID, clientId)
                .log("Connecting to broker");
        reconnectAndResubscribe();
    }

    private synchronized void doConnect() throws MqttException, KeyStoreException, CrtRuntimeException {
        if (!client.getIsConnected()) {
            client.start();
            LOGGER.atInfo()
                    .kv(BridgeConfig.KEY_BROKER_URI, brokerUri)
                    .kv(BridgeConfig.KEY_CLIENT_ID, clientId)
                    .log("Connected to broker");
        }
    }

    private void reconnectAndResubscribe() {
        int waitBeforeRetry = MIN_WAIT_RETRY_IN_SECONDS;

        while(!client.getIsConnected() && !Thread.currentThread().isInterrupted()) {
            try {
                doConnect();
            } catch (MqttException | KeyStoreException | CrtRuntimeException e) {
                if (Utils.getUltimateCause(e) instanceof InterruptedException) {
                    //TODO: Change this from paho. paho doesn't reset the interrupt flag
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


    @Override
    public void stop() {
        //TODO: Need to unsubscribe from all subscriptions?

        removeMappingAndSubscriptions();

        //TODO: I don't think we need a disconnectPacket
        // DisconnectPacket disconnectPacket = new DisconnectPacket.DisconnectPacketBuilder().build();
        try {
            if(client.getIsConnected()) {
                CompletableFuture<Void> f = new CompletableFuture<>();
                stopFuture.set(f);
                client.stop(null);
            }
        } catch (CrtRuntimeException e) {
            LOGGER.atError().setCause(e).log("Failed to disconnect MQTT5 client");
        }
    }

    @Override
    public MqttMessage convertMessage(Message message) {
        return (MqttMessage) message.toMqtt();
    }
}
