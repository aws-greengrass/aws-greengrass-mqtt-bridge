package com.aws.greengrass.mqtt.bridge.clients;

import com.aws.greengrass.logging.api.LogEventBuilder;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqtt.bridge.BridgeConfig;
import com.aws.greengrass.mqtt.bridge.model.Message;
import com.aws.greengrass.mqtt.bridge.model.MqttMessage;
import com.aws.greengrass.mqttclient.v5.Publish;
import com.aws.greengrass.util.RetryUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import software.amazon.awssdk.crt.CRT;
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
import software.amazon.awssdk.crt.mqtt5.packets.ConnAckPacket;
import software.amazon.awssdk.crt.mqtt5.packets.ConnectPacket;
import software.amazon.awssdk.crt.mqtt5.packets.DisconnectPacket;
import software.amazon.awssdk.crt.mqtt5.packets.PubAckPacket;
import software.amazon.awssdk.crt.mqtt5.packets.PublishPacket;
import software.amazon.awssdk.crt.mqtt5.packets.SubAckPacket;
import software.amazon.awssdk.crt.mqtt5.packets.SubscribePacket;
import software.amazon.awssdk.crt.mqtt5.packets.UnsubAckPacket;
import software.amazon.awssdk.crt.mqtt5.packets.UnsubscribePacket;
import com.aws.greengrass.mqtt.bridge.auth.MQTTClientKeyStore;
import software.amazon.awssdk.crt.mqtt5.packets.UserProperty;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
    private final AtomicBoolean hasConnectedOnce = new AtomicBoolean(false);

    //TODO: Figure out what exception to retry on. Might need to retry on suback
    // we dont know which CRTRuntimeException to retry on
    private final RetryUtils.RetryConfig mqttExceptionRetryConfig =
            RetryUtils.RetryConfig.builder().initialRetryInterval(Duration.ofSeconds(1L))
                    .maxRetryInterval(Duration.ofSeconds(120L)).maxAttempt(Integer.MAX_VALUE)
                    .retryableExceptions(Collections.singletonList(CrtRuntimeException.class)).build();

    private final Object subscribeLock = new Object();
    private CompletableFuture<SubAckPacket> subscribeFuture;

    private final AtomicReference<CompletableFuture<Void>> stopFuture = new AtomicReference<>(null);
    @Getter(AccessLevel.PACKAGE)
    private final Mqtt5ClientOptions.LifecycleEvents connectionEventCallback =
            new Mqtt5ClientOptions.LifecycleEvents() {
        @Override
        public void onAttemptingConnect(Mqtt5Client client,
                                       OnAttemptingConnectReturn onAttemptingConnectReturn) {
            LOGGER.atDebug().log("Attempting to connect to Local Mqtt5 Client");
        }

        @Override
        public void onConnectionSuccess(Mqtt5Client client, OnConnectionSuccessReturn onConnectionSuccessReturn) {
            boolean sessionPresent = onConnectionSuccessReturn.getConnAckPacket().getSessionPresent();
            LOGGER.atInfo()
                    .kv(BridgeConfig.KEY_BROKER_URI, brokerUri)
                    .kv(BridgeConfig.KEY_CLIENT_ID, clientId)
                    .log("Connected to broker");
            // TODO: Dependent on the ConACK packet

            if (hasConnectedOnce.compareAndSet(false, true)) {
                LOGGER.atInfo().kv("sessionPresent", sessionPresent).log("Successfully connected to Local Mqtt5 Client");
            } else {
                LOGGER.atInfo().kv("sessionPresent", sessionPresent).log("Connection resumed");
            }
            resubscribe(sessionPresent);
        }

        @Override
        public void onConnectionFailure(Mqtt5Client client, OnConnectionFailureReturn onConnectionFailureReturn) {
            int errorCode = onConnectionFailureReturn.getErrorCode();
            ConnAckPacket packet = onConnectionFailureReturn.getConnAckPacket();
            LogEventBuilder l = LOGGER.atError().kv("error", CRT.awsErrorString(errorCode));
            if (packet != null) {
                l.kv("reasonCode", packet.getReasonCode().name())
                        .kv("reason", packet.getReasonString());
            }
            l.log("Failed to connect to Local Mqtt5 Client");
        }

        @Override
        public void onDisconnection(Mqtt5Client client, OnDisconnectionReturn onDisconnectionReturn) {
            int errorCode = onDisconnectionReturn.getErrorCode();
            DisconnectPacket packet = onDisconnectionReturn.getDisconnectPacket();
            // Error code 0 means that the disconnection was intentional. We do not need to run callbacks when we
            // purposely interrupt a connection.
            if (errorCode == 0 || packet != null && packet.getReasonCode()
                    .equals(DisconnectPacket.DisconnectReasonCode.NORMAL_DISCONNECTION)) {
                LOGGER.atInfo().log("Connection purposefully interrupted");
            } else {
                LogEventBuilder l = LOGGER.atWarn().kv("error", CRT.awsErrorString(errorCode));
                if (packet != null) {
                    l.kv("reasonCode", packet.getReasonCode().name())
                            .kv("reason", packet.getReasonString());
                }
                l.log("Connection interrupted");
            }
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

    public LocalMqtt5Client(@NonNull URI brokerUri, @NonNull String clientId, MQTTClientKeyStore mqttClientKeyStore,
                             ExecutorService executorService, String hostname, Long port) throws MQTTClientException {
        this.brokerUri = brokerUri;
        this.clientId = clientId;
        this.mqttClientKeyStore = mqttClientKeyStore;
        this.mqttClientKeyStore.listenToCAUpdates(this::reset);

        //TODO: Don't worry about executorService for now
        this.executorService = executorService;

        @NonNull
        Mqtt5ClientOptions.PublishEvents messageHandler =
                (client, publishReturn) -> this.messageHandler.accept(publishToMqttMessage(Publish.fromCrtPublishPacket(
                        publishReturn.getPublishPacket())));

        //TODO: figure out where mqttClientKeyStore ssl stuff goes
        Mqtt5ClientOptions mqtt5ClientOptions =
                new Mqtt5ClientOptions.Mqtt5ClientOptionsBuilder(hostname, port)
                        .withLifecycleEvents(connectionEventCallback)
                        .withPublishEvents(messageHandler)
                        .withSessionBehavior(Mqtt5ClientOptions.ClientSessionBehavior.REJOIN_POST_SUCCESS)
                        .withOfflineQueueBehavior(
                                Mqtt5ClientOptions.ClientOfflineQueueBehavior.FAIL_ALL_ON_DISCONNECT)
                        .withConnectOptions(new ConnectPacket.ConnectPacketBuilder()
                                .withRequestProblemInformation(true)
                                .withClientId(clientId).build())
                        .withMaxReconnectDelayMs((long)MAX_WAIT_RETRY_IN_SECONDS * 1000)
                        .withMinReconnectDelayMs((long)MIN_WAIT_RETRY_IN_SECONDS * 1000)
                        .build();
        try {
            this.client = new Mqtt5Client(mqtt5ClientOptions);
        } catch (CrtRuntimeException e) {
            throw new MQTTClientException("Unable to create an MQTT5 client", e);
        }
    }

    private MqttMessage publishToMqttMessage (Publish publish) {
        return MqttMessage.builder()
                .userProperties(publish.getUserProperties())
                .messageExpiryIntervalSeconds(publish.getMessageExpiryIntervalSeconds())
                .payload(publish.getPayload())
                .contentType(publish.getContentType())
                .correlationData(publish.getCorrelationData())
                .retain(publish.isRetain())
                .payloadFormat(publish.getPayloadFormat())
                .responseTopic(publish.getResponseTopic())
                .topic(publish.getTopic())
                .build();
    }

    private List<UserProperty> convertUserProperty (List<com.aws.greengrass.mqttclient.v5.UserProperty> userProperties) {
        return userProperties.stream()
                .map(obj -> new UserProperty(obj.getKey(), obj.getValue()))
                .collect(Collectors.toList());
    }

    private void reset() {
        // TODO: Follow up with Client Cert changes and reinstalling bridge
    }

    @Override
    public void publish(MqttMessage message) throws MessageClientException {
        if(client.getIsConnected()) {

            PublishPacket publishPacket = new PublishPacket.PublishPacketBuilder()
                    .withPayload(message.getPayload())
                    .withQOS(QOS)
                    .withTopic(message.getTopic())
                    .withRetain(message.isRetain())
                    .withPayloadFormat(PublishPacket.PayloadFormatIndicator.getEnumValueFromInteger(message.getPayloadFormat().getValue()))
                    .withContentType(message.getContentType())
                    .withCorrelationData(message.getCorrelationData())
                    .withResponseTopic(message.getResponseTopic())
                    .withUserProperties(convertUserProperty(message.getUserProperties()))
                    .withMessageExpiryIntervalSeconds(message.getMessageExpiryIntervalSeconds())
                    .build();
            LOGGER.atDebug().kv("topic", message.getTopic()).kv("message", message).log("Publishing message to MQTT "
                    + "topic");
            //TODO: In the future, think about returning future
            CompletableFuture<PublishResult> publishFuture = client.publish(publishPacket);
            //TODO: get puback
            try {
                PublishResult pubReturn = publishFuture.get();
                PubAckPacket pubAckPacket = pubReturn.getResultPubAck();
                //TODO: Fix logging
                LOGGER.atDebug().kv("reason", pubAckPacket.getReasonString()).log("while publishing");
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.atDebug().setCause(e).kv("MqttMessage", message).log("failed to subscribe");
            }

        }
    }
    @Override
    public boolean supportsTopicFilters() {
        return true;
    }

    @Override
    public void updateSubscriptions(Set<String> topics, Consumer<MqttMessage> messageHandler) {
        this.messageHandler = messageHandler;

        this.toSubscribeLocalMqttTopics = new HashSet<>(topics);
        LOGGER.atDebug().kv("topics", topics).log("Updated local MQTT5 topics to subscribe");
        if (client.getIsConnected()) {
            updateSubscriptionsInternal(true);
        }
    }
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    private void updateSubscriptionsInternal(boolean sessionPresent) {
        synchronized (subscribeLock) {
            if (subscribeFuture != null) {
                subscribeFuture.cancel(true);
            }

            if (!sessionPresent) {
                // TODO: Work this out better
                // Need to resubscribe to dropped topics
                toSubscribeLocalMqttTopics.addAll(subscribedLocalMqttTopics);
            }

            Set<String> topicsToRemove = new HashSet<>(subscribedLocalMqttTopics);
            topicsToRemove.removeAll(toSubscribeLocalMqttTopics);

            topicsToRemove.forEach(s -> {
                try {
                    unsubscribe(s);
                    LOGGER.atDebug().kv(TOPIC, s).log("Unsubscribed from topic");
                    subscribedLocalMqttTopics.remove(s);
                } catch (Exception e) {
                    // TODO: Unsubscribe doesn't throw an exception
                    LOGGER.atError().kv(TOPIC, s).setCause(e).log("Unable to unsubscribe");
                    // If we are unable to unsubscribe, leave the topic in the set
                    // so that we can try to remove next time.
                }
            });

            Set<String> topicsToSubscribe = new HashSet<>(toSubscribeLocalMqttTopics);
            topicsToSubscribe.removeAll(subscribedLocalMqttTopics);

            LOGGER.atDebug().kv("topics", topicsToSubscribe).log("Subscribing to MQTT topics");

            subscribeToTopics(topicsToSubscribe);

        }
    }

    private void subscribe(String topic) {
        if (client.getIsConnected()) {
            SubscribePacket subscribePacket = new SubscribePacket.SubscribePacketBuilder()
                    .withSubscription(topic, QOS).build();
            LOGGER.atDebug().kv(TOPIC, topic).log("Subscribing to MQTT topic");
            //TODO: deal with subscribeFuture
            this.subscribeFuture = client.subscribe(subscribePacket);
            try {
                SubAckPacket subAckPacket = this.subscribeFuture.get();
                //TODO: Fix logging
                LOGGER.atDebug().kv("reason", subAckPacket.getReasonString()).log("while subscribing");
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.atDebug().setCause(e).kv(TOPIC, topic).log("failed to subscribe");
            }

        }
    }

    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    private void subscribeToTopics(Set<String> topics) {
        //TODO: Think about making this async (non-blocking)

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
                //TODO: Subscribe doesn't throw an exception
                LOGGER.atError().setCause(e).kv(TOPIC, s).log("Failed to subscribe");
            }
        });
    }

    private void resubscribe(boolean sessionPresent) {
        updateSubscriptionsInternal(sessionPresent);
//        subscribedLocalMqttTopics.clear();
//        updateSubscriptionsInternal();
    }

    private void unsubscribe(String topic) {
        if (client.getIsConnected()) {
            UnsubscribePacket unsubscribePacket =
                    new UnsubscribePacket.UnsubscribePacketBuilder().withSubscription(topic).build();
            LOGGER.atDebug().kv(TOPIC, topic).log("Unsubscribing from MQTT topic");
            CompletableFuture<UnsubAckPacket> unsubscribeFuture = client.unsubscribe(unsubscribePacket);
            try {
                UnsubAckPacket unsubAckPacket = unsubscribeFuture.get();
                //TODO: Fix logging
                LOGGER.atDebug().kv("reason", unsubAckPacket.getReasonString()).log("while unsubscribing");
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.atDebug().setCause(e).kv(TOPIC, topic).log("failed to unsubscribe");
            }
        }
    }

    private synchronized void removeMappingAndSubscriptions() {
        unsubscribeAll();
        subscribedLocalMqttTopics.clear();
    }

    private void unsubscribeAll() {
        LOGGER.atDebug().kv("mapping", subscribedLocalMqttTopics).log("Unsubscribe from local MQTT topics");

        this.subscribedLocalMqttTopics.forEach(this::unsubscribe);
    }

    @Override
    public void start()  {
        client.start();
    }

    @Override
    public void stop() {
        //TODO: IF async, make sure everything is completed before the we stop the client
        removeMappingAndSubscriptions();

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
