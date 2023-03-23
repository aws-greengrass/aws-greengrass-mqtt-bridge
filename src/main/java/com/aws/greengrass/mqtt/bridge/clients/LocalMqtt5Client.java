package com.aws.greengrass.mqtt.bridge.clients;

import com.aws.greengrass.logging.api.LogEventBuilder;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqtt.bridge.BridgeConfig;
import com.aws.greengrass.mqtt.bridge.auth.MQTTClientKeyStore;
import com.aws.greengrass.mqtt.bridge.model.Message;
import com.aws.greengrass.mqtt.bridge.model.MqttMessage;
import com.aws.greengrass.mqttclient.v5.Publish;
import com.aws.greengrass.util.RetryUtils;
import com.aws.greengrass.util.Utils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import software.amazon.awssdk.crt.CRT;
import software.amazon.awssdk.crt.CrtRuntimeException;
import software.amazon.awssdk.crt.io.TlsContext;
import software.amazon.awssdk.crt.io.TlsContextOptions;
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
import software.amazon.awssdk.crt.mqtt5.packets.UserProperty;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static ch.qos.logback.core.net.ssl.SSL.DEFAULT_KEYSTORE_PASSWORD;
import static com.aws.greengrass.mqtt.bridge.auth.MQTTClientKeyStore.KEY_ALIAS;

public class LocalMqtt5Client implements MessageClient<MqttMessage> {

    private static final Logger LOGGER = LogManager.getLogger(LocalMqtt5Client.class);

    private static final String LOG_KEY_TOPIC = "topic";
    private static final String LOG_KEY_REASON_CODES = "reasonCodes";
    private static final String LOG_KEY_REASON = "reason";
    private static final String LOG_KEY_MESSAGE = "message";

    private static final long DEFAULT_TCP_MQTT_PORT = 1883;
    private static final long DEFAULT_SSL_MQTT_PORT = 8883;

    private static final int MIN_WAIT_RETRY_IN_SECONDS = 1;
    private static final int MAX_WAIT_RETRY_IN_SECONDS = 120;

    private volatile Consumer<MqttMessage> messageHandler = m -> {};

    private TlsContext tlsContext;
    private TlsContextOptions tlsContextOptions;

    private final URI brokerUri;
    private final String clientId;
    @Getter
    private final Mqtt5Client client;
    private final MQTTClientKeyStore mqttClientKeyStore;
    private final ExecutorService executorService;
    @Getter(AccessLevel.PROTECTED)
    private final Set<String> subscribedLocalMqttTopics = new HashSet<>();
    private final Set<String> toSubscribeLocalMqttTopics = new HashSet<>();
    private final AtomicBoolean hasConnectedOnce = new AtomicBoolean(false);

    //TODO: Figure out what exception to retry on. Might need to retry on suback
    // we dont know which CRTRuntimeException to retry on
    private final RetryUtils.RetryConfig mqttExceptionRetryConfig =
            RetryUtils.RetryConfig.builder().initialRetryInterval(Duration.ofSeconds(1L))
                    .maxRetryInterval(Duration.ofSeconds(120L)).maxAttempt(Integer.MAX_VALUE)
                    .retryableExceptions(Collections.singletonList(CrtRuntimeException.class)).build();

    private final Object updateSubscriptionsLock = new Object();
    private Future<?> updateSubscriptionsTask;

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

            if (hasConnectedOnce.compareAndSet(false, true)) {
                LOGGER.atInfo().kv("sessionPresent", sessionPresent)
                        .log("Successfully connected to Local Mqtt5 Client");
            } else {
                LOGGER.atInfo().kv("sessionPresent", sessionPresent).log("Connection resumed");
            }

            if (!sessionPresent) {
                // Need to resubscribe to dropped topics
                synchronized (updateSubscriptionsLock) {
                    toSubscribeLocalMqttTopics.addAll(subscribedLocalMqttTopics);
                }
            }

            updateSubscriptionsInternalAsync();
        }

        @Override
        @SuppressWarnings("PMD.DoNotLogWithoutLogging")
        public void onConnectionFailure(Mqtt5Client client, OnConnectionFailureReturn onConnectionFailureReturn) {
            int errorCode = onConnectionFailureReturn.getErrorCode();
            ConnAckPacket packet = onConnectionFailureReturn.getConnAckPacket();
            LogEventBuilder l = LOGGER.atError().kv("error", CRT.awsErrorString(errorCode));
            if (packet != null) {
                l.kv("reasonCode", packet.getReasonCode().name())
                        .kv(LOG_KEY_REASON, packet.getReasonString());
            }
            l.log("Failed to connect to Local Mqtt5 Client");
        }

        @Override
        @SuppressWarnings("PMD.DoNotLogWithoutLogging")
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
                            .kv(LOG_KEY_REASON, packet.getReasonString());
                }
                l.log("Connection interrupted");
            }

            synchronized (updateSubscriptionsLock) {
                if (updateSubscriptionsTask != null) {
                    updateSubscriptionsTask.cancel(true);
                }
            }
        }

        @Override
        public void onStopped(Mqtt5Client client, OnStoppedReturn onStoppedReturn) {
            client.close();
        }
    };

    /**
     * Construct a LocalMqtt5Client.
     *
     * @param brokerUri          broker uri
     * @param clientId           client id
     * @param mqttClientKeyStore KeyStore for MQTT Client
     * @param executorService    Executor service
     * @throws MessageClientException if unable to create client for the mqtt broker
     */
    public LocalMqtt5Client(@NonNull URI brokerUri,
                            @NonNull String clientId,
                            MQTTClientKeyStore mqttClientKeyStore,
                            ExecutorService executorService) throws MessageClientException {
        this.brokerUri = brokerUri;
        this.clientId = clientId;
        this.mqttClientKeyStore = mqttClientKeyStore;
        this.executorService = executorService;

        boolean isSSL = "ssl".equalsIgnoreCase(brokerUri.getScheme());

        long port = brokerUri.getPort();
        if (port < 0) {
            port = isSSL ? DEFAULT_SSL_MQTT_PORT : DEFAULT_TCP_MQTT_PORT;
        }

        Mqtt5ClientOptions.Mqtt5ClientOptionsBuilder builder =
                new Mqtt5ClientOptions.Mqtt5ClientOptionsBuilder(brokerUri.getHost(), port)
                        .withLifecycleEvents(connectionEventCallback)
                        .withPublishEvents((client, publishReturn) ->
                                this.messageHandler.accept(
                                        MqttMessage.fromSpoolerV5Model(
                                                Publish.fromCrtPublishPacket(publishReturn.getPublishPacket()))))
                        .withSessionBehavior(Mqtt5ClientOptions.ClientSessionBehavior.REJOIN_POST_SUCCESS)
                        .withOfflineQueueBehavior(Mqtt5ClientOptions.ClientOfflineQueueBehavior.FAIL_ALL_ON_DISCONNECT)
                        .withConnectOptions(new ConnectPacket.ConnectPacketBuilder()
                                .withRequestProblemInformation(true)
                                .withClientId(clientId).build())
                        // TODO configurable?
                        .withMaxReconnectDelayMs((long)MAX_WAIT_RETRY_IN_SECONDS * 1000)
                        .withMinReconnectDelayMs((long)MIN_WAIT_RETRY_IN_SECONDS * 1000);

        if (isSSL) {
            this.tlsContextOptions = TlsContextOptions.createWithMtlsJavaKeystore(mqttClientKeyStore.getKeyStore(),
                    KEY_ALIAS, DEFAULT_KEYSTORE_PASSWORD);
            this.tlsContext = new TlsContext(tlsContextOptions);
            builder.withTlsContext(tlsContext);
        }

        try {
            this.client = new Mqtt5Client(builder.build());
        } catch (CrtRuntimeException e) {
            throw new MQTTClientException("Unable to create an MQTT5 client", e);
        }
    }

    @Override
    public void publish(MqttMessage message) throws MessageClientException {
        if (!client.getIsConnected()) {
            return;
        }
        PublishPacket publishPacket = new PublishPacket.PublishPacketBuilder()
                .withPayload(message.getPayload())
                .withQOS(QOS.AT_LEAST_ONCE)
                .withTopic(message.getTopic())
                .withRetain(message.isRetain())
                .withPayloadFormat(message.getPayloadFormat() == null ? null
                        : PublishPacket.PayloadFormatIndicator.getEnumValueFromInteger(
                                message.getPayloadFormat().getValue()))
                .withContentType(message.getContentType())
                .withCorrelationData(message.getCorrelationData())
                .withResponseTopic(message.getResponseTopic())
                .withUserProperties(convertUserProperty(message.getUserProperties()))
                .withMessageExpiryIntervalSeconds(message.getMessageExpiryIntervalSeconds())
                .build();
        LOGGER.atDebug().kv(LOG_KEY_TOPIC, message.getTopic()).kv(LOG_KEY_MESSAGE, message)
                .log("Publishing message to MQTT topic");

        try {
            PublishResult publishResult = client.publish(publishPacket).get();
            PubAckPacket pubAckPacket = publishResult.getResultPubAck();
            if (pubAckPacket.getReasonCode().equals(PubAckPacket.PubAckReasonCode.SUCCESS)) {
                LOGGER.atDebug().kv(LOG_KEY_MESSAGE, message).log("Message published successfully");
            } else {
                LOGGER.atDebug().kv(LOG_KEY_MESSAGE, message).kv("reasonString", pubAckPacket.getReasonString())
                        .kv("reasonCode", pubAckPacket.getReasonCode()).log("Message failed to publish");
            }
        } catch (ExecutionException e) {
            LOGGER.atDebug().setCause(Utils.getUltimateCause(e)).kv(LOG_KEY_MESSAGE, message)
                    .log("failed to subscribe");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private List<UserProperty> convertUserProperty(List<com.aws.greengrass.mqttclient.v5.UserProperty> userProperties) {
        return userProperties.stream()
                .map(p -> new UserProperty(p.getKey(), p.getValue()))
                .collect(Collectors.toList());
    }

    @Override
    public void updateSubscriptions(Set<String> topics, Consumer<MqttMessage> messageHandler) {
        this.messageHandler = messageHandler;
        synchronized (updateSubscriptionsLock) {
            toSubscribeLocalMqttTopics.clear();
            toSubscribeLocalMqttTopics.addAll(topics);

            LOGGER.atDebug().kv("topics", topics).log("Updated local MQTT5 topics to subscribe");
            if (client.getIsConnected()) {
                updateSubscriptionsInternalAsync();
            }
        }
    }

    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    private void updateSubscriptionsInternalAsync() {
        synchronized (updateSubscriptionsLock) {
            if (updateSubscriptionsTask != null) {
                updateSubscriptionsTask.cancel(true);
            }

            Set<String> topicsToRemove = new HashSet<>(subscribedLocalMqttTopics);
            topicsToRemove.removeAll(toSubscribeLocalMqttTopics);

            Set<String> topicsToSubscribe = new HashSet<>(toSubscribeLocalMqttTopics);
            topicsToSubscribe.removeAll(topicsToRemove);
            topicsToSubscribe.removeAll(subscribedLocalMqttTopics);

            updateSubscriptionsTask = executorService.submit(() -> {
                for (String topic : topicsToRemove) {
                    if (Thread.currentThread().isInterrupted()) {
                        return;
                    }
                    unsubscribe(topic);
                    // TODO retry unsubscribe failures
                }
                LOGGER.atDebug().kv("topics", topicsToSubscribe).log("Subscribing to MQTT topics");
                subscribeToTopics(topicsToSubscribe);
            });
        }
    }

    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    private void subscribeToTopics(Set<String> topics) {
        for (String topic : topics) {
            try {
                RetryUtils.runWithRetry(mqttExceptionRetryConfig, () -> {
                    subscribe(topic);
                    return null;
                }, "subscribe-mqtt5-topic", LOGGER);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                LOGGER.atError().setCause(e).kv(LOG_KEY_TOPIC, topic).log("Failed to subscribe");
            }
        }
    }

    private void subscribe(String topic) {
        if (!client.getIsConnected()) {
            return;
        }
        SubscribePacket subscribePacket = new SubscribePacket.SubscribePacketBuilder()
                // TODO other mqtt5-specific fields
                .withSubscription(topic, QOS.AT_LEAST_ONCE).build();
        LOGGER.atDebug().kv(LOG_KEY_TOPIC, topic).log("Subscribing to MQTT topic");
        try {
            SubAckPacket subAckPacket = client.subscribe(subscribePacket).get();
            if (subAckPacket.getReasonCodes().stream().allMatch(this::subscriptionIsSuccessful)) {
                synchronized (updateSubscriptionsLock) {
                    subscribedLocalMqttTopics.add(topic);
                }
                LOGGER.atDebug()
                        .kv(LOG_KEY_REASON_CODES, subAckPacket.getReasonCodes())
                        .kv(LOG_KEY_REASON, subAckPacket.getReasonString())
                        .kv(LOG_KEY_TOPIC, topic)
                        .log("Successfully subscribed to topic");
            } else {
                LOGGER.atDebug()
                        .kv(LOG_KEY_REASON_CODES, subAckPacket.getReasonCodes())
                        .kv(LOG_KEY_REASON, subAckPacket.getReasonString())
                        .kv(LOG_KEY_TOPIC, topic)
                        .log("Failed to subscribe to topic");
            }
        } catch (ExecutionException e) {
            LOGGER.atDebug().setCause(Utils.getUltimateCause(e)).kv(LOG_KEY_TOPIC, topic)
                    .log("failed to subscribe");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean subscriptionIsSuccessful(SubAckPacket.SubAckReasonCode rc) {
        return rc == SubAckPacket.SubAckReasonCode.GRANTED_QOS_0
                || rc == SubAckPacket.SubAckReasonCode.GRANTED_QOS_1
                || rc == SubAckPacket.SubAckReasonCode.GRANTED_QOS_2;
    }

    private void unsubscribe(String topic) {
        if (!client.getIsConnected()) {
            return;
        }
        UnsubscribePacket unsubscribePacket =
                new UnsubscribePacket.UnsubscribePacketBuilder().withSubscription(topic).build();
        LOGGER.atDebug().kv(LOG_KEY_TOPIC, topic).log("Unsubscribing from MQTT topic");
        try {
            UnsubAckPacket unsubAckPacket = client.unsubscribe(unsubscribePacket).get();
            if (!unsubAckPacket.getReasonCodes().contains(UnsubAckPacket.UnsubAckReasonCode.SUCCESS)) {
                LOGGER.atDebug().kv(LOG_KEY_TOPIC, topic).kv("reasonString",unsubAckPacket.getReasonString()).kv(
                        "reasonCodes", unsubAckPacket.getReasonCodes()).log("failed to unsubscribe");
                return;
            }
            LOGGER.atDebug().kv(LOG_KEY_TOPIC, topic).log("Unsubscribed from topic");
            synchronized (updateSubscriptionsLock) {
                subscribedLocalMqttTopics.remove(topic);
            }
        } catch (ExecutionException e) {
            LOGGER.atDebug().setCause(Utils.getUltimateCause(e)).kv(LOG_KEY_TOPIC, topic).log("failed to unsubscribe");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void unsubscribeAll() {
        synchronized (updateSubscriptionsLock) {
            Set<String> toUnsubscribe = new HashSet<>(subscribedLocalMqttTopics);
            LOGGER.atDebug().kv("topicsToUnsubscribe", toUnsubscribe).log("Unsubscribe from local MQTT topics");
            toUnsubscribe.forEach(this::unsubscribe);
            subscribedLocalMqttTopics.clear();
        }
    }

    @Override
    public void start()  {
        client.start();
    }

    @Override
    public void stop() {
        synchronized (updateSubscriptionsLock) {
            if (updateSubscriptionsTask != null) {
                updateSubscriptionsTask.cancel(true);
            }
            unsubscribeAll();
        }
        try {
            client.stop(null);
        } catch (CrtRuntimeException e) {
            LOGGER.atError().setCause(e).log("Failed to stop MQTT5 client");
        }
        if (this.tlsContext != null) {
            this.tlsContext.close();
        }
        if (this.tlsContextOptions != null) {
            this.tlsContextOptions.close();
        }
    }

    @Override
    public MqttMessage convertMessage(Message message) {
        return (MqttMessage) message.toMqtt();
    }

    @Override
    public boolean supportsTopicFilters() {
        return true;
    }
}
