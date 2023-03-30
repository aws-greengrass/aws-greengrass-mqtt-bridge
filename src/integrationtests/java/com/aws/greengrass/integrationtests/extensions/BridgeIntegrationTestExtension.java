/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests.extensions;


import com.aws.greengrass.clientdevices.auth.CertificateManager;
import com.aws.greengrass.clientdevices.auth.api.ClientDevicesAuthServiceApi;
import com.aws.greengrass.clientdevices.auth.exception.CertificateGenerationException;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.lifecyclemanager.GlobalStateChangeListener;
import com.aws.greengrass.lifecyclemanager.GreengrassService;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.lifecyclemanager.exceptions.ServiceLoadException;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqtt.bridge.BridgeConfig;
import com.aws.greengrass.mqtt.bridge.MQTTBridge;
import com.aws.greengrass.mqtt.bridge.auth.MQTTClientKeyStore;
import com.aws.greengrass.mqtt.bridge.clients.MockMqttClient;
import com.aws.greengrass.mqttclient.MqttClient;
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
import org.testcontainers.utility.MountableFile;
import software.amazon.awssdk.crt.Log;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStoreException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

public class BridgeIntegrationTestExtension implements AfterTestExecutionCallback, InvocationInterceptor {

    private static final Logger logger = LogManager.getLogger(BridgeIntegrationTestExtension.class);

    BridgeIntegrationTestContext context;

    Kernel kernel;

    HiveMQContainer v5Broker;

    Server v3Broker;

    MQTTClientKeyStore clientKeyStore = new InitOnceMqttClientKeyStore();

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
        Log.initLoggingToStderr(Log.LogLevel.Trace); // enable crt logs
        initializeContext(extensionContext);

        if (invocationContext.getArguments().stream().anyMatch(Broker.MQTT3::equals)) {
            startMqtt3Broker();
        } else if (invocationContext.getArguments().stream().anyMatch(Broker.MQTT5::equals)) {
            if (!isDockerAvailable()) {
                logger.atWarn().log("Skipping parameterized test for MQTT5 broker, "
                        + "docker not available on platform");
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

        // TODO support for offline scenarios?
        waitForClientsToConnect();

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
        context = new BridgeIntegrationTestContext();
        injectContext(extensionContext, context);

        // relies on UniqueRootPathExtension being run before this extension via the IntegrationTest annotation
        context.setRootDir(Paths.get(System.getProperty("root")));
    }

    private void startKernel(ExtensionContext extensionContext, String configFile) throws InterruptedException {
        System.setProperty("aws.greengrass.scanSelfClasspath", "true");
        System.setProperty("aws.region", "us-west-2"); // nucleus DeviceConfiguration expects a region
        kernel = new Kernel();
        customizeKernelContext();
        startKernelWithConfig(extensionContext, configFile);
        context.setKernel(kernel);
    }

    private void customizeKernelContext() {
        MockMqttClient mockMqttClient = new MockMqttClient(false);
        kernel.getContext().put(MockMqttClient.class, mockMqttClient);
        kernel.getContext().put(MqttClient.class, mockMqttClient.getMqttClient());

        kernel.getContext().put(CertificateManager.class, mock(CertificateManager.class));
        kernel.getContext().put(MQTTClientKeyStore.class, clientKeyStore);
    }

    private void startMqtt3Broker() throws IOException {
        int brokerPort = 8883;
        IConfig brokerConf = new MemoryConfig(new Properties());
        brokerConf.setProperty(BrokerConstants.PORT_PROPERTY_NAME, String.valueOf(brokerPort));
        v3Broker = new Server();
        v3Broker.startServer(brokerConf);
        context.setBrokerHost("localhost");
        context.setBrokerTCPPort(brokerPort);
        context.setBroker(Broker.MQTT3);
    }

    private void startMqtt5Broker() throws KeyStoreException {
        Certs certs = new Certs(clientKeyStore);

        Path serverKeystorePath = context.getRootDir().resolve("server.jks");
        certs.writeServerKeystore(serverKeystorePath);

        v5Broker = new HiveMQContainer(
                DockerImageName.parse("hivemq/hivemq-ce").withTag("2023.2"))
                .withCopyFileToContainer(MountableFile.forHostPath(serverKeystorePath), "/opt/hivemq/server.jks")
                .withHiveMQConfig(MountableFile.forClasspathResource("hivemq/config.xml"))
                .withEnv("SERVER_JKS_PASSWORD", certs.getServerKeystorePassword())
                .withExposedPorts(8883, 1883)
                .withLogLevel(Level.DEBUG);
        v5Broker.start();
        context.setBroker(Broker.MQTT5);
        context.setBrokerHost(v5Broker.getHost());
        context.setBrokerSSLPort(v5Broker.getMappedPort(8883));
        context.setBrokerTCPPort(v5Broker.getMappedPort(1883));
    }

    private String getConfigFile(ExtensionContext extensionContext) {
        WithKernel annotation = extensionContext.getRequiredTestMethod().getAnnotation(WithKernel.class);
        return annotation == null ? null : annotation.value();
    }

    private void injectContext(ExtensionContext extensionContext, BridgeIntegrationTestContext context) {
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
        URL configFile = extensionContext.getRequiredTestClass().getResource(configFileName);
        if (configFile == null) {
            fail("Config file " + configFileName + " not found");
        }

        kernel.parseArgs("-i", configFile.toString());

        CountDownLatch bridgeRunning = new CountDownLatch(1);
        GlobalStateChangeListener listener = (GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(MQTTBridge.SERVICE_NAME) && service.getState().equals(State.RUNNING)) {
                bridgeRunning.countDown();
            }
        };
        kernel.getContext().addGlobalStateChangeListener(listener);
        try {
            kernel.launch();
            assertTrue(bridgeRunning.await(30L, TimeUnit.SECONDS));
        } finally {
            kernel.getContext().removeGlobalStateChangeListener(listener);
        }
    }

    private void pointBridgeToV5Broker() throws ServiceLoadException, InterruptedException {
        // point bridge to broker running in docker
        String scheme = context.getConfig().getBrokerUri().getScheme();
        context.getKernel().locate(MQTTBridge.SERVICE_NAME)
                .getConfig()
                .lookup(CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_BROKER_URI)
                .withValue(String.format("%s://%s:%d",
                        scheme,
                        context.getBrokerHost(),
                        "ssl".equalsIgnoreCase(scheme) ? context.getBrokerSSLPort() : context.getBrokerTCPPort()));

        // wait for bridge to restart
        CountDownLatch bridgeRunning = new CountDownLatch(1);
        kernel.getContext().addGlobalStateChangeListener((GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(MQTTBridge.SERVICE_NAME) && newState.equals(State.RUNNING)) {
                bridgeRunning.countDown();
            }
        });
        assertTrue(bridgeRunning.await(30L, TimeUnit.SECONDS));
    }

    @SuppressWarnings("PMD.TooFewBranchesForASwitchStatement")
    private void waitForClientsToConnect() throws InterruptedException {
        CountDownLatch localClientConnected = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        context.getFromContext(ExecutorService.class).submit(
                () -> {
                    // TODO can improve on this by having clients reported their connected status within bridge itself
                    switch (context.getConfig().getMqttVersion()) {
                        case MQTT5:
                            waitForClientToConnect(
                                    context.getLocalV5Client().getClient()::getIsConnected, error);
                            break;
                        case MQTT3:
                            waitForClientToConnect(
                                    context.getLocalV3Client().getMqttClientInternal()::isConnected, error);
                            break;
                    }
                    localClientConnected.countDown();
                }
        );
        boolean connectedInTime = localClientConnected.await(30L, TimeUnit.SECONDS);
        Exception e = error.get();
        if (e != null) {
            fail(e);
        }
        if (!connectedInTime) {
            fail("timed out waiting for local mqtt client to connect");
        }

        // don't need to wait for iot core client because we mock iot core
    }

    private void waitForClientToConnect(Supplier<Boolean> connected, AtomicReference<Exception> error) {
        while (!connected.get()) {
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                error.set(e);
                return;
            }
        }
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

    // otherwise, certs will be cleared out when bridge starts-up
    public static class InitOnceMqttClientKeyStore extends MQTTClientKeyStore {
        private final AtomicBoolean initialized = new AtomicBoolean();

        public InitOnceMqttClientKeyStore() {
            super(mock(ClientDevicesAuthServiceApi.class));
        }

        @Override
        public void init() throws KeyStoreException, CertificateGenerationException {
            if (initialized.compareAndSet(false, true)) {
                super.init();
            }
        }
    }
}
