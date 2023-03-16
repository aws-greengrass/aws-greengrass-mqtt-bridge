/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.clientdevices.auth.CertificateManager;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.lifecyclemanager.GlobalStateChangeListener;
import com.aws.greengrass.lifecyclemanager.GreengrassService;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.mqtt.bridge.BridgeConfig;
import com.aws.greengrass.mqtt.bridge.MQTTBridge;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.UniqueRootPathExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.event.Level;
import org.testcontainers.hivemq.HiveMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@Testcontainers
@ExtendWith({GGExtension.class, UniqueRootPathExtension.class, MockitoExtension.class})
public class Mqtt5POCTest {

    @Container
    final HiveMQContainer broker = new HiveMQContainer(
            DockerImageName.parse("hivemq/hivemq-ce").withTag("2021.3"))
            .withLogLevel(Level.DEBUG);

    @TempDir
    Path rootDir;

    Kernel kernel;

    @BeforeEach
    void setup() {
        // Set this property for kernel to scan its own classpath to find plugins
        System.setProperty("aws.greengrass.scanSelfClasspath", "true");

        kernel = new Kernel();
        kernel.getContext().put(CertificateManager.class, mock(CertificateManager.class));
    }

    @AfterEach
    void cleanup() {
        kernel.shutdown();
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
            assertTrue(bridgeRunning.await(30L, TimeUnit.SECONDS));
        } finally {
            kernel.getContext().removeGlobalStateChangeListener(listener);
        }
    }

    @Test
    void GIVEN_mqtt5_broker_WHEN_bridge_starts_THEN_bridge_connects() throws Exception {
        startKernelWithConfig("config.yaml");

        // point bridge to broker running in docker
        kernel.locate(MQTTBridge.SERVICE_NAME)
                .getConfig()
                .lookup(CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_BROKER_URI)
                .withValue(String.format("tcp://%s:%d", broker.getHost(), broker.getMqttPort()));

        // wait for bridge to restart
        CountDownLatch bridgeRunning = new CountDownLatch(1);
        kernel.getContext().addGlobalStateChangeListener((GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(MQTTBridge.SERVICE_NAME) && newState.equals(State.RUNNING)) {
                bridgeRunning.countDown();
            }
        });
        assertTrue(bridgeRunning.await(30L, TimeUnit.SECONDS));
    }
}
