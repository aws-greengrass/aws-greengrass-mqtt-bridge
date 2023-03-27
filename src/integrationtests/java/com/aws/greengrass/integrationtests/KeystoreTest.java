/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.clientdevices.auth.ClientDevicesAuthService;
import com.aws.greengrass.clientdevices.auth.certificate.CertificateHelper;
import com.aws.greengrass.clientdevices.auth.certificate.CertificateStore;
import com.aws.greengrass.config.Topic;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.integrationtests.extensions.BridgeIntegrationTest;
import com.aws.greengrass.integrationtests.extensions.BridgeIntegrationTestContext;
import com.aws.greengrass.integrationtests.extensions.Broker;
import com.aws.greengrass.integrationtests.extensions.TestWithMqtt3Broker;
import com.aws.greengrass.integrationtests.extensions.WithKernel;
import com.aws.greengrass.lifecyclemanager.GlobalStateChangeListener;
import com.aws.greengrass.lifecyclemanager.GreengrassService;
import com.aws.greengrass.mqtt.bridge.BridgeConfig;
import com.aws.greengrass.mqtt.bridge.MQTTBridge;
import com.aws.greengrass.mqtt.bridge.auth.MQTTClientKeyStore;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.lifecyclemanager.GreengrassService.RUNTIME_STORE_NAMESPACE_TOPIC;
import static com.aws.greengrass.lifecyclemanager.GreengrassService.SERVICES_NAMESPACE_TOPIC;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@BridgeIntegrationTest
public class KeystoreTest {
    private static final long AWAIT_TIMEOUT_SECONDS = 30L;

    BridgeIntegrationTestContext testContext;

    @TestWithMqtt3Broker
    @WithKernel("config.yaml")
    void GIVEN_mqtt_bridge_WHEN_cda_ca_conf_changed_THEN_bridge_keystore_updated(Broker broker) throws Exception {
        CountDownLatch keyStoreUpdated = new CountDownLatch(1);
        MQTTClientKeyStore keyStore = testContext.getKernel().getContext().get(MQTTClientKeyStore.class);
        keyStore.listenToCAUpdates(keyStoreUpdated::countDown);

        Topic certificateAuthoritiesTopic = testContext.getKernel().getConfig().lookup(
                SERVICES_NAMESPACE_TOPIC,
                ClientDevicesAuthService.CLIENT_DEVICES_AUTH_SERVICE_NAME,
                RUNTIME_STORE_NAMESPACE_TOPIC,
                ClientDevicesAuthService.CERTIFICATES_KEY,
                ClientDevicesAuthService.AUTHORITIES_TOPIC
        );

        // update topic with CA
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

        assertTrue(keyStoreUpdated.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }

    @TestWithMqtt3Broker
    @WithKernel("config.yaml")
    void GIVEN_mqtt_bridge_WHEN_cda_ca_conf_changed_after_shutdown_THEN_bridge_keystore_not_updated(Broker broker, ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, IllegalArgumentException.class);
        ignoreExceptionOfType(context, NullPointerException.class);

        CountDownLatch keyStoreUpdated = new CountDownLatch(1);
        MQTTClientKeyStore keyStore = testContext.getKernel().getContext().get(MQTTClientKeyStore.class);
        keyStore.listenToCAUpdates(keyStoreUpdated::countDown);

        Topic certificateAuthoritiesTopic = testContext.getKernel().getConfig().lookup(
                SERVICES_NAMESPACE_TOPIC,
                ClientDevicesAuthService.CLIENT_DEVICES_AUTH_SERVICE_NAME,
                RUNTIME_STORE_NAMESPACE_TOPIC,
                ClientDevicesAuthService.CERTIFICATES_KEY,
                ClientDevicesAuthService.AUTHORITIES_TOPIC
        );

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

        // update topic with CA
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

        // shouldn't update
        assertFalse(keyStoreUpdated.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }
}
