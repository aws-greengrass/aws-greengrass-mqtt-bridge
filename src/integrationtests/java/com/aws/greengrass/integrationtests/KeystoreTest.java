/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.clientdevices.auth.ClientDevicesAuthService;
import com.aws.greengrass.clientdevices.auth.api.CertificateUpdateEvent;
import com.aws.greengrass.clientdevices.auth.certificate.CertificateHelper;
import com.aws.greengrass.clientdevices.auth.certificate.CertificateStore;
import com.aws.greengrass.config.Topic;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.integrationtests.extensions.BridgeIntegrationTest;
import com.aws.greengrass.integrationtests.extensions.BridgeIntegrationTestContext;
import com.aws.greengrass.integrationtests.extensions.Broker;
import com.aws.greengrass.integrationtests.extensions.Certs;
import com.aws.greengrass.integrationtests.extensions.TestWithMqtt3Broker;
import com.aws.greengrass.integrationtests.extensions.TestWithMqtt5Broker;
import com.aws.greengrass.integrationtests.extensions.WithKernel;
import com.aws.greengrass.lifecyclemanager.GlobalStateChangeListener;
import com.aws.greengrass.lifecyclemanager.GreengrassService;
import com.aws.greengrass.mqtt.bridge.BridgeConfig;
import com.aws.greengrass.mqtt.bridge.MQTTBridge;
import com.aws.greengrass.mqtt.bridge.auth.MQTTClientKeyStore;
import com.aws.greengrass.util.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.lifecyclemanager.GreengrassService.RUNTIME_STORE_NAMESPACE_TOPIC;
import static com.aws.greengrass.lifecyclemanager.GreengrassService.SERVICES_NAMESPACE_TOPIC;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static com.aws.greengrass.testcommons.testutilities.TestUtils.asyncAssertOnConsumer;
import static com.github.grantwest.eventually.EventuallyLambdaMatcher.eventuallyEval;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@BridgeIntegrationTest
public class KeystoreTest {
    private static final long AWAIT_TIMEOUT_SECONDS = 30L;

    ExecutorService executor = Executors.newCachedThreadPool();
    BridgeIntegrationTestContext testContext;

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @TestWithMqtt5Broker
    @WithKernel("mqtt5_config_ssl.yaml")
    void GIVEN_mqtt_bridge_WHEN_client_cert_changes_THEN_local_client_restarts(Broker broker) throws Exception {
        CountDownLatch clientReset = new CountDownLatch(1);
        executor.submit(() -> {
            assertThat("client disconnected", () -> testContext.getLocalV5Client().getClient().getIsConnected(), eventuallyEval(is(false)));
            assertThat("client reconnected", () -> testContext.getLocalV5Client().getClient().getIsConnected(), eventuallyEval(is(true)));
            clientReset.countDown();
        });

        // rotate client cert
        Certs.RotationResult client = testContext.getCerts().rotateClientCert();
        testContext.getFromContext(MQTTClientKeyStore.class)
                .updateCert(new CertificateUpdateEvent(
                        client.getKp(),
                        client.getCert(),
                        client.getCaCerts()));

        // verify local mqtt5 client reset due to client cert change
        assertTrue(clientReset.await(5L, TimeUnit.SECONDS));
    }

    @TestWithMqtt3Broker
    @WithKernel("config.yaml")
    void GIVEN_mqtt_bridge_WHEN_cda_ca_conf_changed_THEN_bridge_keystore_updated(Broker broker) throws Exception {
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

    @TestWithMqtt5Broker
    @WithKernel("mqtt5_config_ssl.yaml")
    void GIVEN_mqtt_bridge_with_ssl_WHEN_startup_THEN_success(Broker broker) {
    }

    @TestWithMqtt3Broker
    @WithKernel("config.yaml")
    void GIVEN_mqtt_bridge_WHEN_cda_ca_conf_changed_after_shutdown_THEN_bridge_keystore_not_updated(Broker broker, ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, IllegalArgumentException.class);
        ignoreExceptionOfType(context, NullPointerException.class);

        // break bridge
        CountDownLatch bridgeIsBroken = new CountDownLatch(1);
        GlobalStateChangeListener listener = (GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(MQTTBridge.SERVICE_NAME) && service.getState().equals(State.BROKEN)) {
                bridgeIsBroken.countDown();
            }
        };
        Topic brokerUriTopic = testContext.getKernel().getConfig().lookup(
                SERVICES_NAMESPACE_TOPIC,
                MQTTBridge.SERVICE_NAME,
                CONFIGURATION_CONFIG_KEY,
                BridgeConfig.KEY_BROKER_URI
        );
        brokerUriTopic.withValue("garbage");
        testContext.getKernel().getContext().addGlobalStateChangeListener(listener);
        assertTrue(bridgeIsBroken.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));

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

        // shouldn't update
        assertFalse(keyStoreUpdated.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }
}
