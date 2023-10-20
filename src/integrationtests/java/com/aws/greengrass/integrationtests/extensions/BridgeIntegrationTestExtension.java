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
import com.aws.greengrass.mqtt.bridge.model.MqttVersion;
import com.aws.greengrass.mqttclient.MqttClient;
import io.moquette.BrokerConstants;
import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import lombok.RequiredArgsConstructor;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.platform.commons.util.AnnotationUtils;
import org.slf4j.event.Level;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.hivemq.HiveMQContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import software.amazon.awssdk.crt.CrtRuntimeException;

import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.ConnectException;
import java.net.URL;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStoreException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionUltimateCauseOfType;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

public class BridgeIntegrationTestExtension implements AfterTestExecutionCallback, InvocationInterceptor, TestTemplateInvocationContextProvider {

    private static final Logger logger = LogManager.getLogger(BridgeIntegrationTestExtension.class);

    BridgeIntegrationTestContext context;

    Kernel kernel;

    HiveMQContainer v5Broker;

    Server v3Broker;

    MQTTClientKeyStore clientKeyStore = new InitOnceMqttClientKeyStore();

    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        return context.getTestMethod().isPresent()
                && AnnotationUtils.isAnnotated(context.getTestMethod().get(), BridgeIntegrationTest.class);
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
        BridgeIntegrationTest annotation = context.getRequiredTestMethod().getAnnotation(BridgeIntegrationTest.class);
        String configFile = annotation.withConfig();
        Broker[] brokerParams = annotation.withBrokers();
        MqttVersion[] versionParams = annotation.withLocalClientVersions();
        List<BridgeIntegrationTestInvocationContext> contexts = new ArrayList<>();

        if (brokerParams.length == 0 && versionParams.length == 0) {
            contexts.add(new BridgeIntegrationTestInvocationContext(null, null, configFile));
        } else if (versionParams.length == 0) {
            for (Broker brokerParam : brokerParams) {
                contexts.add(new BridgeIntegrationTestInvocationContext(brokerParam, null, configFile));
            }
        } else if (brokerParams.length == 0) {
            for (MqttVersion versionParam : versionParams) {
                contexts.add(new BridgeIntegrationTestInvocationContext(null, versionParam, configFile));
            }
        } else {
            for (Broker brokerParam : brokerParams) {
                for (MqttVersion versionParam : versionParams) {
                    contexts.add(new BridgeIntegrationTestInvocationContext(brokerParam, versionParam, configFile));
                }
            }
        }

        return contexts.stream().map(TestTemplateInvocationContext.class::cast);
    }

    @RequiredArgsConstructor
    @SuppressWarnings("PMD.AvoidUncheckedExceptionsInSignatures")
    class BridgeIntegrationTestParameterResolver implements ParameterResolver {
        @Override
        public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
            return parameterContext.getParameter().getType() == BridgeIntegrationTestContext.class;
        }
        @Override
        public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
            return context;
        }
    }

    @RequiredArgsConstructor
    class BridgeIntegrationTestInvocationContext implements TestTemplateInvocationContext {
        private final Broker broker;
        private final MqttVersion clientVersion;
        private final String configFile;

        @Override
        public String getDisplayName(int invocationIndex) {
            // initialize the context for parameterized test, regardless if
            // BridgeIntegrationTestContext is a param of the test itself.
            // there didn't appear to be a better place to put this...
            if (context == null) {
                context = new BridgeIntegrationTestContext();
                context.broker = broker;
                context.clientVersion = clientVersion;
                context.configFile = configFile;
            }
            return new BridgeIntegrationTestDisplayNameFormatter(broker, clientVersion, configFile).format();
        }

        @Override
        public List<Extension> getAdditionalExtensions() {
            return Collections.singletonList(new BridgeIntegrationTestParameterResolver());
        }
    }

    @RequiredArgsConstructor
    static class BridgeIntegrationTestDisplayNameFormatter {
        private final Broker broker;
        private final MqttVersion clientVersion;
        private final String config;

        @SuppressWarnings("PMD.UseStringBufferForStringAppends")
        String format() {
            String s = String.format("%s broker, %s local client",
                    broker == null ? "No": broker.name(),
                    clientVersion == null ? "default": clientVersion.name());
            if (config != null) {
                s += ", ";
                s += config;
            }
            return s;
        }
    }

    @Override
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    public void interceptTestTemplateMethod(Invocation<Void> invocation,
                                            ReflectiveInvocationContext<Method> invocationContext,
                                            ExtensionContext extensionContext) throws Throwable {
        initializeContext(extensionContext);

        if (context.broker == Broker.MQTT3) {
            configureContextForMqtt3Broker();
        } else if (context.broker == Broker.MQTT5) {
            if (!isDockerAvailable()) {
                logger.atWarn().log("Skipping parameterized test for MQTT5 broker, "
                        + "docker not available on platform");
                invocation.skip();
                return;
            }
            configureContextForMqtt5Broker();
        }

        if (context.broker != null) {
            context.startBroker();
        }

        if (context.configFile == null || context.configFile.isEmpty()) {
            invocation.proceed();
            return;
        }

        // start kernel
        startKernel(extensionContext, context.configFile);
        ignoreExceptionUltimateCauseOfType(extensionContext, ConnectException.class);

        // if local client version is set in BridgeIntegrationTest annotation,
        // make sure config reflects that.
        overrideLocalClientVersion();

        // hack to set bridge to use mqtt5 broker after startup,
        // since testcontainers picks a dynamic broker port
        if (context.broker == Broker.MQTT5) {
            pointBridgeToV5Broker();
            // if test decides to start the broker again,
            // make sure that we re-point bridge to the broker
            // if port changes
            Runnable startBroker = context.startBroker;
            context.startBroker = () -> {
                startBroker.run();
                try {
                    pointBridgeToV5Broker();
                } catch (Exception e) {
                    fail(e);
                }
            };
        }

        if (context.broker != null) {
            // TODO support for offline scenarios?
            waitForClientsToConnect();
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
        injectContext(extensionContext, context);

        // ignore exceptions that can happen during disconnects
        ignoreExceptionOfType(extensionContext, CrtRuntimeException.class);
        ignoreExceptionOfType(extensionContext, MqttException.class);
        ignoreExceptionUltimateCauseOfType(extensionContext, EOFException.class);

        // when checking docker availability
        ignoreExceptionOfType(extensionContext, ClosedChannelException.class);

        // relies on UniqueRootPathExtension being run before this extension via the IntegrationTest annotation
        context.setRootDir(Paths.get(System.getProperty("root")));
    }

    private void startKernel(ExtensionContext extensionContext, String configFile) throws InterruptedException {
        System.setProperty("aws.greengrass.scanSelfClasspath", "true");
        System.setProperty("aws.region", "us-west-2"); // nucleus DeviceConfiguration expects a region
        kernel = new Kernel();
        customizeKernelContext(kernel);
        startKernelWithConfig(extensionContext, configFile);
        context.setKernel(kernel);
    }

    private void customizeKernelContext(Kernel kernel) {
        MockMqttClient mockMqttClient = new MockMqttClient(false);
        kernel.getContext().put(MockMqttClient.class, mockMqttClient);
        kernel.getContext().put(MqttClient.class, mockMqttClient.getMqttClient());

        kernel.getContext().put(CertificateManager.class, mock(CertificateManager.class));
        kernel.getContext().put(MQTTClientKeyStore.class, clientKeyStore);
    }

    private void configureContextForMqtt3Broker() {
        int brokerPort = 8883;
        IConfig brokerConf = new MemoryConfig(new Properties());
        brokerConf.setProperty(BrokerConstants.PORT_PROPERTY_NAME, String.valueOf(brokerPort));
        v3Broker = new Server();
        context.setBrokerHost("localhost");
        context.setBrokerTCPPort(brokerPort);
        context.startBroker = () -> {
            try {
                v3Broker.startServer(brokerConf);
            } catch (IOException e) {
                fail(e);
            }
        };
        context.stopBroker = v3Broker::stopServer;
    }

    private void configureContextForMqtt5Broker() throws KeyStoreException {
        Path serverKeystorePath = context.getRootDir().resolve("hivemq.jks");
        Path serverTruststorePath = context.getRootDir().resolve("truststore.jks");

        Certs certs = new Certs(clientKeyStore, serverKeystorePath, serverTruststorePath);
        certs.initialize();
        context.certs = certs;

        v5Broker = new HiveMQContainer(
                DockerImageName.parse("hivemq/hivemq-ce").withTag("2023.2"))
                .withFileSystemBind(serverKeystorePath.toAbsolutePath().toString(), "/opt/hivemq/hivemq.jks", BindMode.READ_ONLY)
                .withFileSystemBind(serverTruststorePath.toAbsolutePath().toString(), "/opt/hivemq/truststore.jks", BindMode.READ_ONLY)
                .withHiveMQConfig(MountableFile.forClasspathResource("hivemq/config.xml"))
                .withEnv("SERVER_JKS_PASSWORD", certs.getServerKeystorePassword())
                .withExposedPorts(8883, 1883)
                .withLogLevel(Level.DEBUG);
        context.startBroker = () -> {
            v5Broker.start();
            context.setBrokerHost(v5Broker.getHost());
            context.setBrokerSSLPort(v5Broker.getMappedPort(8883));
            context.setBrokerTCPPort(v5Broker.getMappedPort(1883));
        };
        context.stopBroker = v5Broker::stop;
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

    private void overrideLocalClientVersion() throws ServiceLoadException {
        if (context.clientVersion != null) {
            context.getKernel().locate(MQTTBridge.SERVICE_NAME)
                    .getConfig()
                    .lookup(CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_MQTT, BridgeConfig.KEY_VERSION)
                    .withValue(context.clientVersion.name());
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
        // v5 client resets by itself, give time for disconnect to happen
        // TODO find a more reliable way
        Thread.sleep(Duration.ofSeconds(5).toMillis());
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
