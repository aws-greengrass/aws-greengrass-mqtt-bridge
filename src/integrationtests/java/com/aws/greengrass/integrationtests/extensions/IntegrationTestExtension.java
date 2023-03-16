/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests.extensions;


import com.aws.greengrass.clientdevices.auth.CertificateManager;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.lifecyclemanager.GlobalStateChangeListener;
import com.aws.greengrass.lifecyclemanager.GreengrassService;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.lifecyclemanager.exceptions.ServiceLoadException;
import com.aws.greengrass.mqtt.bridge.BridgeConfig;
import com.aws.greengrass.mqtt.bridge.MQTTBridge;
import io.moquette.BrokerConstants;
import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;
import org.slf4j.event.Level;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.hivemq.HiveMQContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

public class IntegrationTestExtension implements AfterTestExecutionCallback, InvocationInterceptor {

    IntegrationTestContext context;

    Kernel kernel;

    HiveMQContainer v5Broker;

    Server v3Broker;

    @Override
    public void interceptTestMethod(Invocation<Void> invocation, ReflectiveInvocationContext<Method> invocationContext,
                                    ExtensionContext extensionContext) throws Throwable {
        initializeContext(extensionContext);
        String configFile = getConfigFile(extensionContext);
        if (configFile == null) {
            invocation.proceed();
            return;
        }
        startKernel(extensionContext, configFile);
        invocation.proceed();
    }

    @Override
    public void interceptTestTemplateMethod(Invocation<Void> invocation,
                                            ReflectiveInvocationContext<Method> invocationContext,
                                            ExtensionContext extensionContext) throws Throwable {
        initializeContext(extensionContext);

        if (invocationContext.getArguments().stream().anyMatch(Broker.MQTT3::equals)) {
            startMqtt3Broker();
        } else if (invocationContext.getArguments().stream().anyMatch(Broker.MQTT5::equals)) {
            if (!isDockerAvailable()) {
                invocation.skip();
                return;
            }
            startMqtt5Broker();
        } else if (extensionContext.getRequiredTestMethod().getAnnotation(TestWithAllBrokers.class) != null
                || extensionContext.getRequiredTestMethod().getAnnotation(TestWithMqtt3Broker.class) != null
                || extensionContext.getRequiredTestMethod().getAnnotation(TestWithMqtt5Broker.class) != null) {
            fail("Please add an argument of type " + Broker.class.getSimpleName()
                    + " to parameterized test " + extensionContext.getRequiredTestMethod().getName()
                    + ". It's required by IntegrationTestExtension to determine which broker to spin up");
        }

        String configFile = getConfigFile(extensionContext);
        if (configFile == null) {
            invocation.proceed();
            return;
        }

        // start kernel
        startKernel(extensionContext, configFile);
        // hack to set bridge to use mqtt5 broker after startup,
        // since testcontainers picks a dynamic broker port
        if (context.broker == Broker.MQTT5) {
            pointBridgeToV5Broker();
        }

        invocation.proceed();
    }

    @Override
    public void afterTestExecution(ExtensionContext extensionContext) {
        if (kernel != null) {
            kernel.shutdown();
        }
        if (v5Broker != null) {
            v5Broker.stop();
        }
        if (v3Broker != null) {
            v3Broker.stopServer();
        }
    }

    private void initializeContext(ExtensionContext extensionContext) {
        context = new IntegrationTestContext();
        injectContext(extensionContext, context);

        // relies on UniqueRootPathExtension being run before this extension via the IntegrationTest annotation
        context.setRootDir(Paths.get(System.getProperty("root")));
    }

    private void startKernel(ExtensionContext extensionContext, String configFile) throws InterruptedException {
        System.setProperty("aws.greengrass.scanSelfClasspath", "true");
        kernel = new Kernel();
        kernel.getContext().put(CertificateManager.class, mock(CertificateManager.class));
        startKernelWithConfig(extensionContext, configFile);
        context.setKernel(kernel);
    }

    private void startMqtt3Broker() throws IOException {
        int brokerPort = 8883;
        IConfig brokerConf = new MemoryConfig(new Properties());
        brokerConf.setProperty(BrokerConstants.PORT_PROPERTY_NAME, String.valueOf(brokerPort));
        v3Broker = new Server();
        v3Broker.startServer(brokerConf);
        context.setBrokerHost("localhost");
        context.setBrokerPort(brokerPort);
        context.setBroker(Broker.MQTT3);
    }

    private void startMqtt5Broker() {
        v5Broker = new HiveMQContainer(
                DockerImageName.parse("hivemq/hivemq-ce").withTag("2021.3"))
                .withLogLevel(Level.DEBUG);
        v5Broker.start();
        context.setBroker(Broker.MQTT5);
        context.setBrokerHost(v5Broker.getHost());
        context.setBrokerPort(v5Broker.getMqttPort());
    }

    private String getConfigFile(ExtensionContext extensionContext) {
        WithKernel annotation = extensionContext.getRequiredTestMethod().getAnnotation(WithKernel.class);
        return annotation == null ? null : annotation.value();
    }

    private void injectContext(ExtensionContext extensionContext, IntegrationTestContext context) {
        Arrays.stream(extensionContext.getRequiredTestClass().getDeclaredFields())
                .filter(f -> f.getType().isAssignableFrom(context.getClass()))
                .forEach(f -> {
                    f.setAccessible(true);
                    try {
                        f.set(extensionContext.getRequiredTestInstance(), context);
                    } catch (IllegalAccessException e) {
                        fail(e);
                    }
                });
    }

    private void startKernelWithConfig(ExtensionContext extensionContext, String configFileName) throws InterruptedException {
        CountDownLatch bridgeRunning = new CountDownLatch(1);
        kernel.parseArgs("-i", extensionContext.getRequiredTestClass().getResource(configFileName).toString());
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

    private void pointBridgeToV5Broker() throws ServiceLoadException, InterruptedException {
        // point bridge to broker running in docker
        context.getKernel().locate(MQTTBridge.SERVICE_NAME)
                .getConfig()
                .lookup(CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_BROKER_URI)
                .withValue(String.format("tcp://%s:%d", context.getBrokerHost(), context.getBrokerPort()));

        // wait for bridge to restart
        CountDownLatch bridgeRunning = new CountDownLatch(1);
        kernel.getContext().addGlobalStateChangeListener((GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(MQTTBridge.SERVICE_NAME) && newState.equals(State.RUNNING)) {
                bridgeRunning.countDown();
            }
        });
        assertTrue(bridgeRunning.await(30L, TimeUnit.SECONDS));
    }

    // from testcontainers
    @SuppressWarnings("PMD.AvoidCatchingThrowable")
    private boolean isDockerAvailable() {
        try {
            DockerClientFactory.instance().client();
            return true;
        } catch (Throwable ex) {
            return false;
        }
    }
}
