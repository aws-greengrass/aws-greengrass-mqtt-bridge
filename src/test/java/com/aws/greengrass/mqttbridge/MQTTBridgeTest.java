/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge;

import com.aws.greengrass.builtin.services.pubsub.PubSubIPCEventStreamAgent;
import com.aws.greengrass.certificatemanager.CertificateManager;
import com.aws.greengrass.componentmanager.KernelConfigResolver;
import com.aws.greengrass.config.Topic;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.config.UpdateBehaviorTree;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.device.ClientDevicesAuthService;
import com.aws.greengrass.lifecyclemanager.GlobalStateChangeListener;
import com.aws.greengrass.lifecyclemanager.GreengrassService;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.lifecyclemanager.exceptions.ServiceLoadException;
import com.aws.greengrass.mqttbridge.auth.MQTTClientKeyStore;
import com.aws.greengrass.mqttbridge.clients.MQTTClient;
import com.aws.greengrass.mqttbridge.clients.MQTTClientException;
import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import com.aws.greengrass.testcommons.testutilities.TestUtils;
import com.aws.greengrass.util.Utils;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.github.grantwest.eventually.EventuallyLambdaMatcher;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class MQTTBridgeTest extends GGServiceTestUtil {
    private static final long TEST_TIME_OUT_SEC = 30L;

    private Kernel kernel;
    private GlobalStateChangeListener listener;
    private Server broker;
    private ScheduledExecutorService ses;

    @TempDir
    Path rootDir;

    @Mock
    CertificateManager mockCertificateManager;

    @BeforeEach
    void setup() throws IOException {
        kernel = new Kernel();
        kernel.getContext().put(CertificateManager.class, mockCertificateManager);
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
        startKernelWithConfig(configFileName, State.RUNNING);
    }

    private void startKernelWithConfig(String configFileName, State expectedStartingState) throws InterruptedException {
        CountDownLatch bridgeRunning = new CountDownLatch(1);
        kernel.parseArgs("-r", rootDir.toAbsolutePath().toString(), "-i",
                getClass().getResource(configFileName).toString());
        listener = (GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(MQTTBridge.SERVICE_NAME) && service.getState().equals(expectedStartingState)) {
                bridgeRunning.countDown();
            }
        };
        kernel.getContext().addGlobalStateChangeListener(listener);
        kernel.launch();

        assertTrue(bridgeRunning.await(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_start_kernel_THEN_bridge_starts_successfully() throws Exception {
        startKernelWithConfig("config.yaml");
    }

    @Test
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_invalid_credential_config_is_fixed_THEN_bridge_automatically_reinstalls(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, MQTTClientException.class);

        CountDownLatch logLatch = new CountDownLatch(1);
        try (AutoCloseable listener = TestUtils.createCloseableLogListener(message -> {
            if (message.getCause() instanceof MQTTClientException &&
                    message.getCause().getMessage().contains(MQTTClient.ERROR_MISSING_USERNAME)) {
                logLatch.countDown();
            }
        })) {
            startKernelWithConfig("config_with_invalid_credentials.yaml", State.BROKEN);
        }

        assertTrue(logLatch.await(2, TimeUnit.SECONDS));

        CountDownLatch stateChangesOccurred = trackServiceStateChanges(State.NEW, State.RUNNING);
        updateConfig(new String[]{"configuration", "brokerConnectionOptions", "username"}, "a_username");

        assertTrue(stateChangesOccurred.await(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_multiple_config_changes_consecutively_THEN_bridge_reinstalls_once(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, InterruptedException.class);
        startKernelWithConfig("config.yaml");

        CountDownLatch stateChangesOccurred = trackServiceStateChanges(State.NEW, State.RUNNING);
        updateConfig(new String[]{"configuration", "brokerUri"}, "tcp://newbrokeruri");

        assertTrue(stateChangesOccurred.await(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_multiple_serialized_config_changes_occur_THEN_bridge_reinstalls_multiple_times(ExtensionContext context) throws Exception {
        ignoreExceptionOfType(context, InterruptedException.class);
        startKernelWithConfig("config.yaml");

        Semaphore bridgeRestarted = new Semaphore(1);
        bridgeRestarted.acquire();

        kernel.getContext().addGlobalStateChangeListener((GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(MQTTBridge.SERVICE_NAME) && newState.equals(State.RUNNING)) {
                bridgeRestarted.release();
            }
        });

        Topics config = kernel.locate(MQTTBridge.SERVICE_NAME).getConfig()
                .lookupTopics(KernelConfigResolver.CONFIGURATION_CONFIG_KEY);

        UpdateBehaviorTree updateBehavior = new UpdateBehaviorTree(
                UpdateBehaviorTree.UpdateBehavior.MERGE, System.currentTimeMillis());

        int numRestarts = 5;
        for (int i = 0; i < numRestarts; i++) {
            // change the configuration and wait for bridge to restart
            config.updateFromMap(Utils.immutableMap(BridgeConfig.KEY_BROKER_URI, String.format("tcp://brokeruri:%d", i)), updateBehavior);
            assertTrue(bridgeRestarted.tryAcquire(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
        }
    }

    @Test
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_clientId_config_changes_THEN_bridge_reinstalls() throws Exception {
        startKernelWithConfig("config.yaml");

        CountDownLatch stateChangesOccurred = trackServiceStateChanges(State.NEW, State.RUNNING);
        updateConfig(new String[]{"configuration", "clientId"}, "new_client_id");

        assertTrue(stateChangesOccurred.await(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_valid_mqttTopicMapping_updated_THEN_mapping_updated() throws Exception {
        startKernelWithConfig("config.yaml");
        TopicMapping topicMapping = ((MQTTBridge) kernel.locate(MQTTBridge.SERVICE_NAME)).getTopicMapping();
        assertThat(topicMapping.getMapping().size(), is(equalTo(0)));

        CountDownLatch bridgeRestarted = new CountDownLatch(1);
        kernel.getContext().addGlobalStateChangeListener((GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(MQTTBridge.SERVICE_NAME) && newState.equals(State.NEW)) {
                bridgeRestarted.countDown();
            }
        });

        Topics mappingConfigTopics = kernel.locate(MQTTBridge.SERVICE_NAME).getConfig()
                .lookupTopics(KernelConfigResolver.CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_MQTT_TOPIC_MAPPING);

        mappingConfigTopics.replaceAndWait(Utils.immutableMap("m1",
                Utils.immutableMap("topic", "mqtt/topic", "source", TopicMapping.TopicType.LocalMqtt.toString(),
                        "target", TopicMapping.TopicType.IotCore.toString()), "m2",
                Utils.immutableMap("topic", "mqtt/topic2", "source", TopicMapping.TopicType.LocalMqtt.toString(),
                        "target", TopicMapping.TopicType.Pubsub.toString()), "m3",
                Utils.immutableMap("topic", "mqtt/topic3", "source", TopicMapping.TopicType.LocalMqtt.toString(),
                        "target", TopicMapping.TopicType.IotCore.toString())));

        kernel.getContext().runOnPublishQueueAndWait(() -> {
        });
        assertThat(topicMapping.getMapping().size(), is(equalTo(3)));
        assertFalse(bridgeRestarted.await(2, TimeUnit.SECONDS));
    }

    @Test
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_valid_mapping_provided_in_config_THEN_mapping_populated()
            throws Exception {
        startKernelWithConfig("config_with_mapping.yaml");
        TopicMapping topicMapping = ((MQTTBridge) kernel.locate(MQTTBridge.SERVICE_NAME)).getTopicMapping();

        assertThat(() -> topicMapping.getMapping().size(), EventuallyLambdaMatcher.eventuallyEval(is(3)));
        Map<String, TopicMapping.MappingEntry> expectedMapping = new HashMap<>();
        expectedMapping.put("mapping1",
                new TopicMapping.MappingEntry("topic/to/map/from/local/to/cloud", TopicMapping.TopicType.LocalMqtt,
                        TopicMapping.TopicType.IotCore));
        expectedMapping.put("mapping2",
                new TopicMapping.MappingEntry("topic/to/map/from/local/to/pubsub", TopicMapping.TopicType.LocalMqtt,
                        TopicMapping.TopicType.Pubsub));
        expectedMapping.put("mapping3",
                new TopicMapping.MappingEntry("topic/to/map/from/local/to/cloud/2", TopicMapping.TopicType.LocalMqtt,
                        TopicMapping.TopicType.IotCore));

        assertEquals(expectedMapping, topicMapping.getMapping());
    }

    @Test
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_empty_mqttTopicMapping_updated_THEN_mapping_not_updated()
            throws Exception {
        startKernelWithConfig("config.yaml");
        TopicMapping topicMapping = ((MQTTBridge) kernel.locate(MQTTBridge.SERVICE_NAME)).getTopicMapping();
        assertThat(topicMapping.getMapping().size(), is(equalTo(0)));

        kernel.locate(MQTTBridge.SERVICE_NAME).getConfig()
                .lookupTopics(KernelConfigResolver.CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_MQTT_TOPIC_MAPPING)
                .replaceAndWait(Collections.EMPTY_MAP);
        // Block until subscriber has finished updating
        kernel.getContext().runOnPublishQueueAndWait(() -> {
        });
        assertThat(topicMapping.getMapping().size(), is(equalTo(0)));
    }

    @Test
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_mapping_updated_with_empty_THEN_mapping_removed()
            throws Exception {
        startKernelWithConfig("config_with_mapping.yaml");
        TopicMapping topicMapping = ((MQTTBridge) kernel.locate(MQTTBridge.SERVICE_NAME)).getTopicMapping();

        assertThat(() -> topicMapping.getMapping().size(), EventuallyLambdaMatcher.eventuallyEval(is(3)));
        kernel.locate(MQTTBridge.SERVICE_NAME).getConfig()
                .lookupTopics(KernelConfigResolver.CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_MQTT_TOPIC_MAPPING)
                .replaceAndWait(Collections.EMPTY_MAP);
        // Block until subscriber has finished updating
        kernel.getContext().runOnPublishQueueAndWait(() -> {
        });
        assertThat(topicMapping.getMapping().size(), is(equalTo(0)));
    }

    @Test
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_invalid_mqttTopicMapping_updated_THEN_mapping_not_updated(ExtensionContext context)
            throws Exception {
        ignoreExceptionOfType(context, InvalidFormatException.class);
        startKernelWithConfig("config.yaml");
        TopicMapping topicMapping = ((MQTTBridge) kernel.locate(MQTTBridge.SERVICE_NAME)).getTopicMapping();
        assertThat(topicMapping.getMapping().size(), is(equalTo(0)));

        kernel.getContext().removeGlobalStateChangeListener(listener);

        CountDownLatch bridgeErrored = trackServiceStateChanges(State.RUNNING);

        // Updating with invalid mapping (Providing type as Pubsub-Invalid)
        Topics mappingConfigTopics = kernel.locate(MQTTBridge.SERVICE_NAME).getConfig()
                .lookupTopics(KernelConfigResolver.CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_MQTT_TOPIC_MAPPING);

        mappingConfigTopics.replaceAndWait(Utils.immutableMap("m1",
                Utils.immutableMap("topic", "mqtt/topic", "source", TopicMapping.TopicType.LocalMqtt.toString(),
                        "target", TopicMapping.TopicType.IotCore.toString()), "m2",
                Utils.immutableMap("topic", "mqtt/topic2", "source", TopicMapping.TopicType.LocalMqtt.toString(),
                        "target", "Pubsub-Invalid"), "m3",
                Utils.immutableMap("topic", "mqtt/topic3", "source", TopicMapping.TopicType.LocalMqtt.toString(),
                        "target", TopicMapping.TopicType.IotCore.toString())));

        // Block until subscriber has finished updating
        kernel.getContext().runOnPublishQueueAndWait(() -> {
        });
        assertThat(topicMapping.getMapping().size(), is(equalTo(0)));

        assertTrue(bridgeErrored.await(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_CAs_updated_THEN_KeyStore_updated() throws Exception {
        serviceFullName = MQTTBridge.SERVICE_NAME;
        initializeMockedConfig();
        TopicMapping mockTopicMapping = mock(TopicMapping.class);
        MessageBridge mockMessageBridge = mock(MessageBridge.class);
        PubSubIPCEventStreamAgent mockPubSubIPCAgent = mock(PubSubIPCEventStreamAgent.class);
        Kernel mockKernel = mock(Kernel.class);
        MQTTClientKeyStore mockMqttClientKeyStore = mock(MQTTClientKeyStore.class);
        MQTTBridge mqttBridge;

        Topics config = Topics.of(context, KernelConfigResolver.CONFIGURATION_CONFIG_KEY, null);
        config.lookup(KernelConfigResolver.CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_BROKER_URI)
                .dflt("ssl://localhost:8883");
        config.lookup(KernelConfigResolver.CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_CLIENT_ID)
                .dflt(MQTTBridge.SERVICE_NAME);

        try (MqttClient mockIotMqttClient = mock(MqttClient.class)) {
            mqttBridge =
                    new MQTTBridge(config, mockTopicMapping, mockMessageBridge, mockPubSubIPCAgent, mockIotMqttClient,
                            mockKernel, mockMqttClientKeyStore, ses);
            mqttBridge.setMqttClientFactory(() -> mock(MQTTClient.class));
        }

        ClientDevicesAuthService mockClientAuthService = mock(ClientDevicesAuthService.class);
        when(mockKernel.locate(ClientDevicesAuthService.CLIENT_DEVICES_AUTH_SERVICE_NAME))
                .thenReturn(mockClientAuthService);
        Topics mockClientAuthConfig = mock(Topics.class);
        when(mockClientAuthService.getConfig()).thenReturn(mockClientAuthConfig);

        Topic caTopic = Topic.of(context, "authorities", Arrays.asList("CA1", "CA2"));
        when(mockClientAuthConfig
                .lookup(MQTTBridge.RUNTIME_STORE_NAMESPACE_TOPIC, ClientDevicesAuthService.CERTIFICATES_KEY,
                        ClientDevicesAuthService.AUTHORITIES_TOPIC)).thenReturn(caTopic);
        mqttBridge.install();
        mqttBridge.startup();
        mqttBridge.shutdown();
        ArgumentCaptor<List<String>> caListCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockMqttClientKeyStore).updateCA(caListCaptor.capture());
        assertThat(caListCaptor.getValue(), is(Arrays.asList("CA1", "CA2")));

        caTopic = Topic.of(context, "authorities", Collections.emptyList());
        when(mockClientAuthConfig
                .lookup(MQTTBridge.RUNTIME_STORE_NAMESPACE_TOPIC, ClientDevicesAuthService.CERTIFICATES_KEY,
                        ClientDevicesAuthService.AUTHORITIES_TOPIC)).thenReturn(caTopic);
        reset(mockMqttClientKeyStore);
        mqttBridge.install();
        mqttBridge.startup();
        mqttBridge.shutdown();
        verify(mockMqttClientKeyStore, never()).updateCA(caListCaptor.capture());
    }

    private void updateConfig(String[] config, String value) throws ServiceLoadException {
        UpdateBehaviorTree updateBehavior = new UpdateBehaviorTree(
                UpdateBehaviorTree.UpdateBehavior.MERGE, System.currentTimeMillis());

        String[] configPath = Arrays.copyOfRange(config, 0, config.length - 1);
        String configName = config[config.length - 1];

        kernel.locate(MQTTBridge.SERVICE_NAME).getConfig().lookupTopics(configPath)
                .updateFromMap(Utils.immutableMap(configName, value), updateBehavior);
    }

    private CountDownLatch trackServiceStateChanges(State... expectedStates) {
        CountDownLatch stateChangesOccurred = new CountDownLatch(1);

        if (expectedStates == null || expectedStates.length == 0) {
            stateChangesOccurred.countDown();
            return stateChangesOccurred;
        }

        final Queue<State> states = new LinkedList<>(Arrays.asList(expectedStates));

        kernel.getContext().addGlobalStateChangeListener((GreengrassService service, State was, State newState) -> {
            if (!service.getName().equals(MQTTBridge.SERVICE_NAME)) {
                return;
            }
            synchronized (states) {
                if (states.isEmpty()) {
                    return;
                }

                State nextState = states.peek();
                if (newState.equals(nextState)) {
                    states.poll();
                }

                if (states.isEmpty()) {
                    stateChangesOccurred.countDown();
                }
            }
        });
        return stateChangesOccurred;
    }
}
