/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.builtin.services.pubsub.PubSubIPCEventStreamAgent;
import com.aws.greengrass.clientdevices.auth.CertificateManager;
import com.aws.greengrass.clientdevices.auth.api.ClientDevicesAuthServiceApi;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.dependency.ImplementsService;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.mqtt.bridge.MQTTBridge;
import com.aws.greengrass.mqtt.bridge.TopicMapping;
import com.aws.greengrass.mqtt.bridge.auth.MQTTClientKeyStore;
import com.aws.greengrass.mqtt.bridge.clients.LocalMqttClientFactory;
import com.aws.greengrass.mqtt.bridge.clients.MQTTClient;
import com.aws.greengrass.mqtt.bridge.clients.MessageClient;
import com.aws.greengrass.mqtt.bridge.clients.MessageClientException;
import com.aws.greengrass.mqtt.bridge.clients.MockMqttClient;
import com.aws.greengrass.mqtt.bridge.model.BridgeConfigReference;
import com.aws.greengrass.mqtt.bridge.model.MqttMessage;
import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.UniqueRootPathExtension;
import com.aws.greengrass.util.Pair;
import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.inject.Inject;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static com.github.grantwest.eventually.EventuallyLambdaMatcher.eventuallyEval;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

// not using @BridgeIntegrationTest because we're using a custom MQTTBridge service
@ExtendWith({GGExtension.class, UniqueRootPathExtension.class, MockitoExtension.class})
class LifecycleTest {

    Kernel kernel = kernelWithMocks();
    Server moquetteBroker;

    @BeforeEach
    void setUp() throws IOException {
        moquetteBroker = startMoquette();
    }

    @AfterEach
    void tearDown() {
        if (kernel != null) {
            kernel.shutdown();
        }
        if (moquetteBroker != null) {
            moquetteBroker.stopServer();
        }
    }

    @Test
    void GIVEN_bridge_that_reinstalls_during_startup_WHEN_bridge_starts_THEN_no_duplicates_of_message_clients_occur(ExtensionContext context) throws InterruptedException {
        ignoreExceptionOfType(context, MqttException.class);
        CachingLocalMqttClientFactory localClientFactory = kernel.getContext().get(CachingLocalMqttClientFactory.class);
        launchKernel(kernel, context, ReinstallingMQTTBridge.CONFIG_FILE);
        waitForBridgeToRun();
        for (int i = 0; i < 5; i++) {
            assertFalse(disconnectLoopDetected(localClientFactory));
            Thread.sleep(1000L);
        }
    }

    private static boolean disconnectLoopDetected(CachingLocalMqttClientFactory localClientFactory) {
        List<Pair<MQTTClient, ConnectionChangedCallback>> createdClients = localClientFactory.getCreatedClients();
        if (createdClients.size() <= 1) {
            return false;
        }
        return createdClients.stream()
                .anyMatch(p -> p.getRight().getNumConnects().get() > 1
                        || p.getRight().getNumDisconnects().get() > 1);
    }

    private static Kernel kernelWithMocks() {
        Kernel kernel = new Kernel();
        MockMqttClient mockMqttClient = new MockMqttClient(false);
        kernel.getContext().put(MockMqttClient.class, mockMqttClient);
        kernel.getContext().put(MqttClient.class, mockMqttClient.getMqttClient());
        kernel.getContext().put(ClientDevicesAuthServiceApi.class, mock(ClientDevicesAuthServiceApi.class));
        kernel.getContext().put(CertificateManager.class, mock(CertificateManager.class));
        kernel.getContext().put(LocalMqttClientFactory.class, kernel.getContext().get(CachingLocalMqttClientFactory.class));
        return kernel;
    }

    private static void launchKernel(Kernel kernel, ExtensionContext extensionContext, String configFileName) {
        System.setProperty("aws.greengrass.scanSelfClasspath", "true");
        URL configFile = extensionContext.getRequiredTestClass().getResource(configFileName);
        assertNotNull(configFile);
        kernel.parseArgs("-i", configFile.toString());
        kernel.launch();
    }

    private static Server startMoquette() throws IOException {
        IConfig brokerConf = new MemoryConfig(new Properties());
        brokerConf.setProperty(IConfig.PORT_PROPERTY_NAME, String.valueOf(8883));
        brokerConf.setProperty(IConfig.ENABLE_TELEMETRY_NAME, "false");
        Server moquette = new Server();
        moquette.startServer(brokerConf);
        return moquette;
    }

    private void waitForBridgeToRun() {
        ReinstallingMQTTBridge bridge = kernel.getContext().get(ReinstallingMQTTBridge.class);
        assertThat("bridge is running", bridge::getState, eventuallyEval(is(State.RUNNING)));
    }

    @Getter
    @RequiredArgsConstructor
    static class ConnectionChangedCallback implements MqttCallbackExtended {

        private final AtomicInteger numConnects = new AtomicInteger();
        private final AtomicInteger numDisconnects = new AtomicInteger();

        private final MqttCallback existingCallback;

        static ConnectionChangedCallback replaceCallback(MQTTClient client) {
            MqttCallback existingCallback = client.getMqttCallback();
            ConnectionChangedCallback newCallback = new ConnectionChangedCallback(existingCallback);
            client.setMqttCallback(newCallback);
            return newCallback;
        }

        @Override
        public void connectComplete(boolean reconnect, String serverURI) {
            numConnects.incrementAndGet();
        }

        @Override
        public void connectionLost(Throwable cause) {
            numDisconnects.incrementAndGet();
            existingCallback.connectionLost(cause);
        }

        @Override
        public void messageArrived(String topic, org.eclipse.paho.client.mqttv3.MqttMessage message) throws Exception {
            existingCallback.messageArrived(topic, message);
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {
            existingCallback.deliveryComplete(token);
        }
    }

    static class CachingLocalMqttClientFactory extends LocalMqttClientFactory {

        @Getter
        private final List<Pair<MQTTClient, ConnectionChangedCallback>> createdClients = new ArrayList<>();

        @Inject
        public CachingLocalMqttClientFactory(BridgeConfigReference config, MQTTClientKeyStore mqttClientKeyStore, ExecutorService executorService, ScheduledExecutorService ses) {
            super(config, mqttClientKeyStore, executorService, ses);
        }

        @Override
        public MessageClient<MqttMessage> createLocalMqttClient() throws MessageClientException {
            MessageClient<MqttMessage> client = super.createLocalMqttClient();
            if (!(client instanceof MQTTClient)) {
                throw new UnsupportedOperationException("not yet supported in test");
            }
            ConnectionChangedCallback callback = ConnectionChangedCallback.replaceCallback((MQTTClient) client);
            createdClients.add(new Pair<>((MQTTClient) client, callback));
            return client;
        }
    }

    @ImplementsService(name = "aws.greengrass.clientdevices.mqtt.ReinstallingBridge")
    static class ReinstallingMQTTBridge extends MQTTBridge {

        public static final String CONFIG_FILE = "reinstalling_bridge.yaml";

        private final AtomicBoolean reinstallOnStartup = new AtomicBoolean(true);

        @Inject
        public ReinstallingMQTTBridge(Topics topics, TopicMapping topicMapping, PubSubIPCEventStreamAgent pubSubIPCAgent,
                                      MqttClient iotMqttClient, Kernel kernel, MQTTClientKeyStore mqttClientKeyStore,
                                      LocalMqttClientFactory localMqttClientFactory, ExecutorService executorService,
                                      BridgeConfigReference bridgeConfig) {
            super(topics, topicMapping, pubSubIPCAgent, iotMqttClient, kernel,
                    mqttClientKeyStore, localMqttClientFactory, executorService, bridgeConfig);
        }

        @Override
        public synchronized void startup() {
            if (reinstallOnStartup.compareAndSet(true, false)) {
                requestReinstall();
            }
            super.startup();
        }
    }
}
