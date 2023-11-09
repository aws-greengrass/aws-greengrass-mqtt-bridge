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
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.Setter;
import software.amazon.awssdk.crt.mqtt.MqttClientConnection;
import software.amazon.awssdk.crt.mqtt.MqttClientConnectionEvents;
import software.amazon.awssdk.crt.mqtt.QualityOfService;
import software.amazon.awssdk.iot.AwsIotMqttConnectionBuilder;

import java.io.IOException;
import java.net.URI;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateEncodingException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class LocalMqtt3Client implements MessageClient<MqttMessage>, Configurable {

    private static final Logger LOGGER = LogManager.getLogger(LocalMqtt3Client.class);
    private static final String KV_TOPIC = "topic";
    private static final RetryUtils.RetryConfig ALWAYS_RETRY_FOREVER = RetryUtils.RetryConfig.builder()
            .initialRetryInterval(Duration.ofSeconds(1))
            .maxRetryInterval(Duration.ofSeconds(30))
            .maxAttempt(Integer.MAX_VALUE)
            .retryableExceptions(Collections.singletonList(Exception.class))
            .build();

    private final AtomicReference<Config> config = new AtomicReference<>();
    private final Connection connection = new Connection();
    private final SubscribeTask subscribeTask = new SubscribeTask();
    private final ResetTask resetTask = new ResetTask();
    private final ClientState state = new ClientState();
    private final MqttClientFactory clientFactory = new MqttClientFactory();
    private final ConnectionEvents connectionEvents = new ConnectionEvents();
    private final MessageHandler messageHandler = new MessageHandler();
    private final KeyStoreUpdateHandler onKeyStoreUpdate = new KeyStoreUpdateHandler();

    private final MQTTClientKeyStore mqttClientKeyStore;
    private final ExecutorService executor;

    /**
     * Construct a LocalMqtt3Client.
     *
     * @param bridgeConfig       bridge configuration
     * @param mqttClientKeyStore MQTT client keystore
     * @param executor           executor service
     */
    public LocalMqtt3Client(BridgeConfig bridgeConfig,
                            MQTTClientKeyStore mqttClientKeyStore,
                            ExecutorService executor) {
        applyConfig(bridgeConfig);
        this.mqttClientKeyStore = mqttClientKeyStore;
        this.executor = executor;
    }

    public boolean isConnected() {
        return state.get() == State.CONNECTED;
    }

    @Override
    public void start() throws MessageClientException {
        mqttClientKeyStore.listenToUpdates(onKeyStoreUpdate);
        if (!state.set(State.START)) {
            return;
        }
        resetTask.run();
    }

    @Override
    public void stop() {
        if (!state.set(State.STOP)) {
            return;
        }
        mqttClientKeyStore.unsubscribeFromUpdates(onKeyStoreUpdate);
        resetTask.cancel();
        subscribeTask.cancel();
        connection.close();
    }

    @Override
    public void publish(MqttMessage message) throws MessageClientException {
        connection.get()
                .thenCompose(connection ->
                        connection.publish(new software.amazon.awssdk.crt.mqtt.MqttMessage(
                                message.getTopic(),
                                message.getPayload(),
                                QualityOfService.AT_LEAST_ONCE))) // TODO configurable
                .whenComplete((unused, e) -> {
                    if (e != null) {
                        LOGGER.atError().cause(e).kv(KV_TOPIC, message.getTopic())
                                .log("Unable to publish message");
                    }
                });
    }

    @Override
    public void updateSubscriptions(Set<String> topics, Consumer<MqttMessage> messageHandler) {
        this.messageHandler.setHandler(messageHandler);
        subscribeTask.updateSubscriptions(topics);
    }

    @Override
    public void applyConfig(BridgeConfig config) {
        applyConfig(Config.fromBridgeConfig(config));
    }

    @SuppressWarnings("PMD.PrematureDeclaration")
    private void applyConfig(@NonNull Config config) {
        Config previousConfig = this.config.getAndSet(config);

        State state = this.state.get();
        if (state == State.STOP || state == State.NEW) {
            return;
        }

        if (Config.resetRequired(previousConfig, config)) {
            LOGGER.atInfo().kv("config", config)
                    .log("New configuration received, will restart the client for changes to take effect");
            resetTask.run();
        }
    }

    @Override
    public MqttMessage convertMessage(Message message) {
        return (MqttMessage) message.toMqtt();
    }

    private static boolean isDisconnectException(Throwable e) {
        Throwable cause = Utils.getUltimateCause(e);
        return cause instanceof ClientStoppedException || cause instanceof ClientDisconnectedException;
    }

    private static <T> CompletableFuture<T> exceptionalFuture(Exception e) {
        CompletableFuture<T> f = new CompletableFuture<>();
        f.completeExceptionally(e);
        return f;
    }

    static class ClientState {
        private final AtomicReference<State> state = new AtomicReference<>(State.NEW);

        State get() {
            return state.get();
        }

        boolean set(State state) {
            AtomicReference<State> prevState = new AtomicReference<>();
            State newState = this.state.updateAndGet(current -> {
                prevState.set(current);
                // only start() can be happen after a stop().
                // prevents events like reconnect or cert changes
                // from accidentally happening when they shouldn't.
                if (current == State.STOP && state != State.NEW) {
                    return current;
                }
                // allow connect() or stop() after a disconnect().
                if (current == State.HARD_DISCONNECTED && !(state == State.CONNECTING || state == State.STOP)) {
                    return current;
                }
                return state;
            });
            return !Objects.equals(prevState.get(), newState);
        }
    }

    enum State {
        /**
         * Client is in initial state, not yet started or connected.
         */
        NEW,
        /**
         * Client started.
         */
        START,
        /**
         * New connection in progress.
         */
        CONNECTING,
        /**
         * Client is connected to the local broker.
         */
        CONNECTED,
        /**
         * Client is disconnected from the local broker,
         * temporarily.
         */
        DISCONNECTED,
        /**
         * Client explicitly disconnected from the local broker,
         * the connection is no longer usable.
         */
        HARD_DISCONNECTED,
        /**
         * Client is closing and will not accept new work.
         */
        STOP
    }

    @Data
    @Builder(toBuilder = true)
    public static class Config {
        URI brokerUri;
        String clientId;
        long ackTimeoutSeconds;
        long pingTimeoutMs;
        long keepAliveTimeoutSeconds;
        long maxReconnectDelayMs;
        long minReconnectDelayMs;

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
                    .ackTimeoutSeconds(bridgeConfig.getAckTimeoutSeconds())
                    .pingTimeoutMs(bridgeConfig.getPingTimeoutMs())
                    .keepAliveTimeoutSeconds(bridgeConfig.getKeepAliveTimeoutSeconds())
                    .maxReconnectDelayMs(bridgeConfig.getMaxReconnectDelayMs())
                    .minReconnectDelayMs(bridgeConfig.getMinReconnectDelayMs())
                    .build();
        }

        /**
         * Determine if {@link LocalMqtt3Client} needs a restart, based on configuration changes.
         *
         * @param prevConfig previous config
         * @param newConfig  new config
         * @return true if the client needs a restart to apply config changes
         */
        public static boolean resetRequired(Config prevConfig, Config newConfig) {
            return !Objects.equals(prevConfig, newConfig);
        }

        public boolean isSSL() {
            return "ssl".equalsIgnoreCase(getBrokerUri().getScheme());
        }
    }

    class KeyStoreUpdateHandler implements MQTTClientKeyStore.UpdateListener {
        @Override
        public void onCAUpdate() {
            if (ignoreUpdate()) {
                return;
            }
            LOGGER.atInfo().log("New CA cert available, reconnecting client");
            resetTask.run();
        }

        @Override
        public void onClientCertUpdate() {
            if (ignoreUpdate()) {
                return;
            }
            LOGGER.atInfo().log("New client certificate available, reconnecting client");
            resetTask.run();
        }

        private boolean ignoreUpdate() {
            State state = LocalMqtt3Client.this.state.get();
            return !config.get().isSSL() || state == State.STOP || state == State.NEW;
        }
    }

    class ConnectionEvents implements MqttClientConnectionEvents {

        @Override
        public void onConnectionInterrupted(int errorCode) {
            if (state.set(State.DISCONNECTED)) {
                subscribeTask.cancel();
            }
        }

        @Override
        public void onConnectionResumed(boolean sessionPresent) {
            if (state.set(State.CONNECTED)) {
                subscribeTask.resubscribeAll();
            }
        }
    }

    static class MessageHandler implements Consumer<software.amazon.awssdk.crt.mqtt.MqttMessage> {

        @Setter
        private Consumer<MqttMessage> handler;

        @Override
        public void accept(software.amazon.awssdk.crt.mqtt.MqttMessage mqttMessage) {
            Consumer<MqttMessage> handler = this.handler;
            if (handler == null) {
                return;
            }
            handler.accept(MqttMessage.fromCrtMQTT3(mqttMessage));
        }
    }

    class Connection {
        private MqttClientConnection conn;
        private CompletableFuture<MqttClientConnection> task;

        synchronized CompletableFuture<MqttClientConnection> get() {
            State state = LocalMqtt3Client.this.state.get();
            switch (state) {
                case NEW:
                    return exceptionalFuture(new ClientNotStartedException());
                case STOP:
                    return exceptionalFuture(new ClientStoppedException());
                case DISCONNECTED:
                    return exceptionalFuture(new ClientDisconnectedException());
                default:
                    break;
            }

            if (task == null || state == State.HARD_DISCONNECTED) {
                task = newConnection();
            }
            return task;
        }

        @SuppressWarnings("PMD.CloseResource")
        private synchronized CompletableFuture<MqttClientConnection> newConnection() {
            MqttClientConnection conn;
            try {
                conn = clientFactory.apply();
            } catch (MQTTClientException e) {
                return exceptionalFuture(e);
            }
            this.conn = conn;

            LocalMqtt3Client.this.state.set(State.CONNECTING);

            return conn.connect()
                    .thenApply(unused -> conn)
                    .whenComplete((c, e) -> {
                        Config config = LocalMqtt3Client.this.config.get();
                        if (e == null) {
                            LocalMqtt3Client.this.state.set(State.CONNECTED);
                            LOGGER.atDebug()
                                    .kv(BridgeConfig.KEY_BROKER_URI, config.getBrokerUri())
                                    .kv(BridgeConfig.KEY_CLIENT_ID, config.getClientId())
                                    .log("New connection created");
                        } else {
                            LOGGER.atWarn().cause(e)
                                    .kv(BridgeConfig.KEY_BROKER_URI, config.getBrokerUri())
                                    .kv(BridgeConfig.KEY_CLIENT_ID, config.getClientId())
                                    .log("Connection failed");
                            close(c);
                            LocalMqtt3Client.this.state.set(State.HARD_DISCONNECTED);
                        }
                    });
        }

        synchronized void close() {
            if (task != null && !task.isDone()) {
                task.cancel(false);
            }
            close(conn);
            LocalMqtt3Client.this.state.set(State.HARD_DISCONNECTED);
        }

        void close(MqttClientConnection connection) {
            if (connection == null) {
                return;
            }
            connection.close();
        }
    }

    class ResetTask {

        private Future<?> task;

        synchronized void run() {
            cancel();
            if (shouldReset()) {
                task = executor.submit(this::reset);
            }
        }

        synchronized void cancel() {
            if (task != null && !task.isDone()) {
                task.cancel(true);
            }
        }

        @SuppressWarnings("PMD.AvoidCatchingGenericException")
        private void reset() {
            if (!shouldReset()) {
                return;
            }

            State state = LocalMqtt3Client.this.state.get();
            if (state == State.CONNECTED) {
                LOGGER.atWarn().log("Beginning client reset. The client will be offline for a period of time, "
                        + "during which messages will dropped");
            }

            subscribeTask.cancel();
            if (state != State.START) {
                connection.close();
            }

            try {
                RetryUtils.runWithRetry(
                        ALWAYS_RETRY_FOREVER,
                        () -> {
                            connection.get().get();
                            subscribeTask.resubscribeAll();
                            return null;
                        },
                        "reset-connection",
                        LOGGER
                );
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                subscribeTask.cancel(); // TODO?
            } catch (Exception e) {
                // should never happen
                LOGGER.atError().cause(e).log("Unable to reset client connection. "
                        + "Please review errors and restart Greengrass to retry");
            }
        }

        private boolean shouldReset() {
            return LocalMqtt3Client.this.state.get() != State.STOP;
        }
    }

    class SubscribeTask {
        private final Set<String> subscribed = new HashSet<>();
        private final Set<String> pending = new HashSet<>();
        private Future<?> task;

        synchronized void resubscribeAll() {
            subscribed.clear();
            submitSubscribeTask();
        }

        synchronized void updateSubscriptions(Set<String> topics) {
            pending.clear();
            pending.addAll(topics);
            submitSubscribeTask();
        }

        synchronized void cancel() {
            if (task == null || task.isDone()) {
                return;
            }
            task.cancel(true);
        }

        @SuppressWarnings("PMD.AvoidCatchingGenericException")
        private synchronized void submitSubscribeTask() {
            cancel();
            if (shouldStop()) {
                return;
            }

            Set<String> toRemove = new HashSet<>(subscribed);
            toRemove.removeAll(pending);
            Set<String> toSubscribe = new HashSet<>(pending);
            toSubscribe.removeAll(subscribed);

            task = executor.submit(() -> {
                try {
                    RetryUtils.runWithRetry(
                            ALWAYS_RETRY_FOREVER,
                            () -> updateSubscriptionsInternal(toRemove, toSubscribe),
                            "update-subscriptions",
                            LOGGER
                    );
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    // should never happen
                    LOGGER.atError().cause(e).log("Update subscriptions failed. "
                            + "Please review the error and restart or redeploy the MQTT Bridge component to retry");
                }
            });
        }

        private Void updateSubscriptionsInternal(Set<String> toRemove, Set<String> toSubscribe)
                throws ExecutionException, InterruptedException, TimeoutException {
            for (String topic : toRemove) {
                if (shouldStop()) {
                    return null;
                }
                subscribeSync(topic);
            }
            for (String topic: toSubscribe) {
                if (shouldStop()) {
                    return null;
                }
                unsubscribeSync(topic);
            }
            return null;
        }

        private void subscribeSync(String topic) throws ExecutionException, InterruptedException, TimeoutException {
            waitFor(subscribe(topic));
        }

        private CompletableFuture<Integer> subscribe(String topic) {
            return connection.get()
                    // TODO configurable qos
                    .thenCompose(connection -> connection.subscribe(topic, QualityOfService.AT_LEAST_ONCE))
                    .handle((unused, e) -> {
                        if (e == null) {
                            synchronized (this) {
                                subscribed.add(topic);
                            }
                            LOGGER.atDebug().kv(KV_TOPIC, topic)
                                    .log("Subscription created");
                        } else if (!isDisconnectException(e)) { // subscriptions are cleared on disconnect
                            LOGGER.atError().cause(e).kv(KV_TOPIC, topic)
                                    .log("Unable to subscribe");
                            throw new CompletionException(e);
                        }
                        return unused;
                    });
        }

        private void unsubscribeSync(String topic) throws ExecutionException, InterruptedException, TimeoutException {
            waitFor(unsubscribe(topic));
        }

        private CompletableFuture<Integer> unsubscribe(String topic) {
            return connection.get()
                    .thenCompose(connection -> connection.unsubscribe(topic))
                    .handle((unused, e) -> {
                        if (e == null) {
                            synchronized (this) {
                                subscribed.remove(topic);
                            }
                            LOGGER.atDebug().kv(KV_TOPIC, topic)
                                    .log("Subscription removed");
                        } else if (!isDisconnectException(e)) { // subscriptions are cleared on disconnect
                            LOGGER.atError().cause(e).kv(KV_TOPIC, topic)
                                    .log("Unable to unsubscribe");
                            throw new CompletionException(e);
                        }
                        return unused;
                    });
        }

        private boolean shouldStop() {
            return Thread.currentThread().isInterrupted() || state.get() != State.CONNECTED;
        }

        private void waitFor(CompletableFuture<?> future)
                throws ExecutionException, InterruptedException, TimeoutException {
            future.get(config.get().getAckTimeoutSeconds(), TimeUnit.SECONDS);
        }
    }

    class MqttClientFactory implements CrashableSupplier<MqttClientConnection, MQTTClientException> {

        @Override
        public MqttClientConnection apply() throws MQTTClientException {
            AwsIotMqttConnectionBuilder builder;
            Config config = LocalMqtt3Client.this.config.get();
            if (config.isSSL()) {
                try {
                    builder = AwsIotMqttConnectionBuilder.newMtlsBuilder(
                                    mqttClientKeyStore.getCertPem(),
                                    mqttClientKeyStore.getKeyPem())
                            .withCertificateAuthority(mqttClientKeyStore.getCaCertsAsString()
                                    .orElseThrow(() -> new MQTTClientException(
                                            "unable to create mqtt3 client, missing ca certs")));
                } catch (KeyStoreException | CertificateEncodingException | IOException
                         | UnrecoverableKeyException | NoSuchAlgorithmException e) {
                    throw new MQTTClientException("unable to create mqtt3 client with ssl", e);
                }
            } else {
                builder = null;
                // TODO build connection directly
            }

            try {
                builder.withEndpoint(config.getBrokerUri().getHost())
                        .withPort((short) getPort(config)) // TODO this is a bug
                        .withClientId(config.getClientId())
                        .withCleanSession(true)
                        .withConnectionEventCallbacks(connectionEvents)
                        .withKeepAliveSecs((int) config.getKeepAliveTimeoutSeconds())
                        .withPingTimeoutMs((int) config.getPingTimeoutMs())
                        .withProtocolOperationTimeoutMs(
                                (int) Duration.ofSeconds(config.getAckTimeoutSeconds()).toMillis())
                        .withReconnectTimeoutSecs(
                                (int) config.getMinReconnectDelayMs() / 1000,
                                (int) config.getMaxReconnectDelayMs() / 1000);

                MqttClientConnection connection = builder.build();
                connection.onMessage(messageHandler);
                return connection;
            } finally {
                builder.close();
            }
        }

        @SuppressWarnings("PMD.AvoidUsingShortType")
        private int getPort(Config config) {
            int port = config.getBrokerUri().getPort();
            if (port < 0) {
                port = config.isSSL() ? 8883 : 1883;
            }
            return port;
        }
    }

    // TODO refactor to own package

    private static class ClientStoppedException extends MQTTClientException {
        private static final long serialVersionUID = 9051259301498127368L;

        public ClientStoppedException() {
            super("Client stopped");
        }
    }

    private static class ClientDisconnectedException extends MQTTClientException {
        private static final long serialVersionUID = 9057828941556618755L;

        public ClientDisconnectedException() {
            super("Client temporarily disconnected");
        }
    }

    private static class ClientNotStartedException extends MQTTClientException {
        private static final long serialVersionUID = 9051259301498127368L;

        public ClientNotStartedException() {
            super("Client not started");
        }
    }
}
