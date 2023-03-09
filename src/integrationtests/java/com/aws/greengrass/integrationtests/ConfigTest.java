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
import com.aws.greengrass.mqtt.bridge.MQTTBridge;
import com.aws.greengrass.mqtt.bridge.MQTTBridgeTest;
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
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@ExtendWith({GGExtension.class, UniqueRootPathExtension.class, MockitoExtension.class})
public class ConfigTest {
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
                MQTTBridgeTest.class.getResource(configFileName).toString());
        GlobalStateChangeListener listener = (GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(MQTTBridge.SERVICE_NAME) && service.getState().equals(State.RUNNING)) {
                bridgeRunning.countDown();
            }
        };
        kernel.getContext().addGlobalStateChangeListener(listener);
        kernel.launch();
        assertTrue(bridgeRunning.await(10, TimeUnit.SECONDS));
    }

    @Test // TODO add other tests
    void GIVEN_bridge_WHEN_kernel_started_THEN_bridge_starts() throws Exception {
        startKernelWithConfig("config.yaml");
    }

}
