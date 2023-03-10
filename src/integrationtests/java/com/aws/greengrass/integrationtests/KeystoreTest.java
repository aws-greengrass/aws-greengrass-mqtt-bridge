/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.clientdevices.auth.CertificateManager;
import com.aws.greengrass.clientdevices.auth.ClientDevicesAuthService;
import com.aws.greengrass.clientdevices.auth.certificate.CertificateHelper;
import com.aws.greengrass.clientdevices.auth.certificate.CertificateStore;
import com.aws.greengrass.config.Topic;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.lifecyclemanager.GlobalStateChangeListener;
import com.aws.greengrass.lifecyclemanager.GreengrassService;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.mqtt.bridge.BridgeConfig;
import com.aws.greengrass.mqtt.bridge.MQTTBridge;
import com.aws.greengrass.mqtt.bridge.auth.MQTTClientKeyStore;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.UniqueRootPathExtension;
import io.moquette.BrokerConstants;
import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.lifecyclemanager.GreengrassService.RUNTIME_STORE_NAMESPACE_TOPIC;
import static com.aws.greengrass.lifecyclemanager.GreengrassService.SERVICES_NAMESPACE_TOPIC;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@ExtendWith({GGExtension.class, UniqueRootPathExtension.class, MockitoExtension.class})
public class KeystoreTest {

    @TempDir
    Path rootDir;

    Kernel kernel;

    // TODO mqtt5
    Server broker;

    // TODO consider executor service or same thread executor
    ScheduledExecutorService ses;

    @BeforeEach
    void setup() throws IOException {
        // Set this property for kernel to scan its own classpath to find plugins
        System.setProperty("aws.greengrass.scanSelfClasspath", "true");

        kernel = new Kernel();
        kernel.getContext().put(CertificateManager.class, mock(CertificateManager.class));

        IConfig defaultConfig = new MemoryConfig(new Properties());
        defaultConfig.setProperty(BrokerConstants.PORT_PROPERTY_NAME, "8883");
        broker = new Server();
        broker.startServer(defaultConfig);

        ses = new ScheduledThreadPoolExecutor(1);
    }

    @AfterEach
    void cleanup() {
        kernel.shutdown();
        broker.stopServer();
        ses.shutdownNow();
    }

    private void startKernelWithConfig(String configFileName) throws InterruptedException {
        CountDownLatch bridgeRunning = new CountDownLatch(1);
        kernel.parseArgs("-r", rootDir.toAbsolutePath().toString(), "-i",
                getClass().getResource(configFileName).toString());
        GlobalStateChangeListener listener = (GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(MQTTBridge.SERVICE_NAME) && service.getState().equals(State.RUNNING)) {
                bridgeRunning.countDown();
            }
        };
        try {
            kernel.getContext().addGlobalStateChangeListener(listener);
            kernel.launch();
            assertTrue(bridgeRunning.await(10, TimeUnit.SECONDS));
        } finally {
            kernel.getContext().removeGlobalStateChangeListener(listener);
        }
    }

    @Test
    void GIVEN_mqtt_bridge_WHEN_cda_ca_conf_changed_THEN_bridge_keystore_updated() throws Exception {
        startKernelWithConfig("config.yaml");

        CountDownLatch keyStoreUpdated = new CountDownLatch(1);
        MQTTClientKeyStore keyStore = kernel.getContext().get(MQTTClientKeyStore.class);
        keyStore.listenToCAUpdates(keyStoreUpdated::countDown);

        Topic certificateAuthoritiesTopic = kernel.getConfig().lookup(
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

        assertTrue(keyStoreUpdated.await(5L, TimeUnit.SECONDS));
    }

    @Test
    void GIVEN_mqtt_bridge_WHEN_cda_ca_conf_changed_after_shutdown_THEN_bridge_keystore_not_updated(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, IllegalArgumentException.class);
        ignoreExceptionOfType(context, NullPointerException.class);

        startKernelWithConfig("config.yaml");

        CountDownLatch keyStoreUpdated = new CountDownLatch(1);
        MQTTClientKeyStore keyStore = kernel.getContext().get(MQTTClientKeyStore.class);
        keyStore.listenToCAUpdates(keyStoreUpdated::countDown);

        Topic certificateAuthoritiesTopic = kernel.getConfig().lookup(
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
        Topic brokerUriTopic = kernel.getConfig().lookup(
                SERVICES_NAMESPACE_TOPIC,
                MQTTBridge.SERVICE_NAME,
                CONFIGURATION_CONFIG_KEY,
                BridgeConfig.KEY_BROKER_URI
        );
        brokerUriTopic.withValue("garbage");
        kernel.getContext().addGlobalStateChangeListener(listener);
        assertTrue(bridgeIsBroken.await(10L, TimeUnit.SECONDS));

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
        assertFalse(keyStoreUpdated.await(5L, TimeUnit.SECONDS));
    }
}
