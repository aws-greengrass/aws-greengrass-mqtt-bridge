/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.clientdevices.auth.ClientDevicesAuthService;
import com.aws.greengrass.clientdevices.auth.certificate.CertificateHelper;
import com.aws.greengrass.clientdevices.auth.certificate.CertificateStore;
import com.aws.greengrass.config.Topic;
import com.aws.greengrass.integrationtests.extensions.BridgeIntegrationTest;
import com.aws.greengrass.integrationtests.extensions.BridgeIntegrationTestContext;
import com.aws.greengrass.integrationtests.extensions.Broker;
import com.aws.greengrass.mqtt.bridge.MQTTBridge;
import com.aws.greengrass.mqtt.bridge.auth.MQTTClientKeyStore;
import com.aws.greengrass.mqtt.bridge.model.MqttVersion;
import com.aws.greengrass.util.Pair;
import lombok.Builder;
import lombok.Setter;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtensionContext;
import software.amazon.awssdk.crt.mqtt5.Mqtt5Client;
import software.amazon.awssdk.crt.mqtt5.Mqtt5ClientOptions;
import software.amazon.awssdk.crt.mqtt5.OnAttemptingConnectReturn;
import software.amazon.awssdk.crt.mqtt5.OnConnectionFailureReturn;
import software.amazon.awssdk.crt.mqtt5.OnConnectionSuccessReturn;
import software.amazon.awssdk.crt.mqtt5.OnDisconnectionReturn;
import software.amazon.awssdk.crt.mqtt5.OnStoppedReturn;

import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.aws.greengrass.lifecyclemanager.GreengrassService.RUNTIME_STORE_NAMESPACE_TOPIC;
import static com.aws.greengrass.lifecyclemanager.GreengrassService.SERVICES_NAMESPACE_TOPIC;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static com.aws.greengrass.testcommons.testutilities.TestUtils.asyncAssertOnConsumer;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class KeystoreTest {
    private static final long AWAIT_TIMEOUT_SECONDS = 30L;
    BridgeIntegrationTestContext testContext;

    @Disabled("use for manual memory leak testing")
    @BridgeIntegrationTest(
            withConfig = "mqtt5_config_ssl.yaml",
            withBrokers = Broker.MQTT5)
    void GIVEN_mqtt5_bridge_WHEN_client_cert_changes_THEN_memory_does_not_leak() throws Exception {
        while (true) {
            testContext.getCerts().rotateClientCert();
            Thread.sleep(5000L);
        }
    }

    @BridgeIntegrationTest(
            withConfig = "mqtt5_config_ssl.yaml",
            withBrokers = Broker.MQTT5)
    void GIVEN_mqtt5_bridge_WHEN_client_cert_changes_THEN_local_client_restarts() throws Exception {
        CompletableFuture<Void> numConnects = asyncAssertNumConnects(1);
        testContext.getCerts().rotateClientCert();
        testContext.getCerts().waitForBrokerToApplyStoreChanges();
        numConnects.get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertTrue(testContext.getLocalV5Client().getClient().getIsConnected());
    }

    @BridgeIntegrationTest(
            withConfig = "mqtt3_config_ssl.yaml",
            withBrokers = Broker.MQTT3, // TODO hivemq doesn't play nicely with v3 paho client for some reason
            withLocalClientVersions = MqttVersion.MQTT3)
    void GIVEN_bridge_with_ssl_WHEN_bridge_starts_THEN_client_connects_over_ssl() {
    }

    @BridgeIntegrationTest(
            withConfig = "mqtt5_config_ssl.yaml",
            withBrokers = Broker.MQTT5)
    void GIVEN_mqtt5_bridge_WHEN_ca_changes_THEN_local_client_restarts() throws Exception {
        CompletableFuture<Void> numConnects = asyncAssertNumConnects(1);
        testContext.getCerts().rotateCA();
        testContext.getCerts().rotateServerCert();
        testContext.getCerts().rotateClientCert();
        testContext.getCerts().waitForBrokerToApplyStoreChanges();
        numConnects.get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertTrue(testContext.getLocalV5Client().getClient().getIsConnected());
    }

    @BridgeIntegrationTest(
            withConfig = "mqtt5_config_ssl.yaml",
            withBrokers = Broker.MQTT5)
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    void GIVEN_mqtt5_bridge_WHEN_client_cert_changes_a_lot_THEN_local_client_is_connected() throws Exception {
        IntStream.range(0, 30).forEach(i -> {
            try {
                testContext.getCerts().rotateClientCert();
            } catch (Exception e) {
                fail(e);
            }
        });
        testContext.getCerts().waitForBrokerToApplyStoreChanges();
        assertTrue(testContext.getLocalV5Client().getClient().getIsConnected());
    }

    @BridgeIntegrationTest(
            withConfig = "config.yaml",
            withBrokers = Broker.MQTT3)
    void GIVEN_mqtt_bridge_WHEN_cda_ca_conf_changed_THEN_bridge_keystore_updated() throws Exception {
        Pair<CompletableFuture<Void>, Consumer<Void>> keystoreUpdated = asyncAssertOnConsumer(p -> {}, 1);
        MQTTClientKeyStore keyStore = testContext.getKernel().getContext().get(MQTTClientKeyStore.class);
        keyStore.listenToUpdates(() -> keystoreUpdated.getRight().accept(null));

        Topic certificateAuthoritiesTopic = testContext.getKernel().getConfig().lookup(
                SERVICES_NAMESPACE_TOPIC,
                ClientDevicesAuthService.CLIENT_DEVICES_AUTH_SERVICE_NAME,
                RUNTIME_STORE_NAMESPACE_TOPIC,
                ClientDevicesAuthService.CERTIFICATES_KEY,
                ClientDevicesAuthService.AUTHORITIES_TOPIC
        );

        // update topic with invalid content
        certificateAuthoritiesTopic.withValue("garbage");
        testContext.getKernel().getContext().waitForPublishQueueToClear();

        // update topic with valid CA
        certificateAuthoritiesTopic.withValue(
                Collections.singletonList(
                        CertificateHelper.toPem(
                                CertificateHelper.createCACertificate(
                                        CertificateStore.newRSAKeyPair(2048),
                                        Date.from(Instant.now()),
                                        Date.from(Instant.now().plusSeconds(100)),
                                        "CA"
                                ))
                ));

        keystoreUpdated.getLeft().get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @BridgeIntegrationTest(
            withConfig = "config.yaml",
            withBrokers = Broker.MQTT3)
    void GIVEN_mqtt_bridge_WHEN_cda_ca_conf_changed_after_shutdown_THEN_bridge_keystore_not_updated(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, IllegalArgumentException.class);
        ignoreExceptionOfType(context, NullPointerException.class);

        // shutdown the bridge
        testContext.getFromContext(MQTTBridge.class).shutdown();

        CountDownLatch keyStoreUpdated = new CountDownLatch(1);
        MQTTClientKeyStore keyStore = testContext.getKernel().getContext().get(MQTTClientKeyStore.class);
        keyStore.listenToUpdates(keyStoreUpdated::countDown);

        // update topic with CA
        Topic certificateAuthoritiesTopic = testContext.getKernel().getConfig().lookup(
                SERVICES_NAMESPACE_TOPIC,
                ClientDevicesAuthService.CLIENT_DEVICES_AUTH_SERVICE_NAME,
                RUNTIME_STORE_NAMESPACE_TOPIC,
                ClientDevicesAuthService.CERTIFICATES_KEY,
                ClientDevicesAuthService.AUTHORITIES_TOPIC
        );
        certificateAuthoritiesTopic.withValue(
                Collections.singletonList(CertificateHelper.toPem(
                        CertificateHelper.createCACertificate(
                                CertificateStore.newRSAKeyPair(2048),
                                Date.from(Instant.now()),
                                Date.from(Instant.now().plusSeconds(100)),
                                "CA"))));

        testContext.getKernel().getContext().waitForPublishQueueToClear();
        assertFalse(keyStoreUpdated.await(5L, TimeUnit.SECONDS));
    }

    private CompletableFuture<Void> asyncAssertNumConnects(Integer numConnects) throws InterruptedException {
        CountDownLatch callbackApplied = new CountDownLatch(1);
        Pair<CompletableFuture<Void>, Consumer<Void>> onConnect
                = asyncAssertOnConsumer(unused -> callbackApplied.countDown(),
                numConnects + 1); // one call taken up by restarting for callback to take effect
        testContext.getLocalV5Client().setConnectionEventCallback(
                DelegatingConnectionEventsCallback.builder()
                        .lifecycleEvents(testContext.getLocalV5Client().getConnectionEventCallback())
                        .onConnectionSuccess(onConnect)
                        .build());
        // need to reset for new callback to take effect
        testContext.getLocalV5Client().reset();
        assertTrue(callbackApplied.await(5L, TimeUnit.SECONDS));
        return onConnect.getLeft();
    }

    @Builder
    static class DelegatingConnectionEventsCallback implements Mqtt5ClientOptions.LifecycleEvents {

        private final Mqtt5ClientOptions.LifecycleEvents lifecycleEvents;
        @Setter
        private Pair<CompletableFuture<Void>, Consumer<Void>> onConnectionSuccess;

        @Override
        public void onAttemptingConnect(Mqtt5Client mqtt5Client, OnAttemptingConnectReturn onAttemptingConnectReturn) {
            lifecycleEvents.onAttemptingConnect(mqtt5Client, onAttemptingConnectReturn);
        }

        @Override
        public void onConnectionSuccess(Mqtt5Client mqtt5Client, OnConnectionSuccessReturn onConnectionSuccessReturn) {
            if (onConnectionSuccess != null) {
                onConnectionSuccess.getRight().accept(null);
            }
            lifecycleEvents.onConnectionSuccess(mqtt5Client, onConnectionSuccessReturn);
        }

        @Override
        public void onConnectionFailure(Mqtt5Client mqtt5Client, OnConnectionFailureReturn onConnectionFailureReturn) {
            lifecycleEvents.onConnectionFailure(mqtt5Client, onConnectionFailureReturn);
        }

        @Override
        public void onDisconnection(Mqtt5Client mqtt5Client, OnDisconnectionReturn onDisconnectionReturn) {
            lifecycleEvents.onDisconnection(mqtt5Client, onDisconnectionReturn);
        }

        @Override
        public void onStopped(Mqtt5Client mqtt5Client, OnStoppedReturn onStoppedReturn) {
            lifecycleEvents.onStopped(mqtt5Client, onStoppedReturn);
        }
    }
}
