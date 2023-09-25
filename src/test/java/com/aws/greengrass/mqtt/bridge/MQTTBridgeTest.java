/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge;

import com.aws.greengrass.builtin.services.pubsub.PubSubIPCEventStreamAgent;
import com.aws.greengrass.clientdevices.auth.certificate.CertificateHelper;
import com.aws.greengrass.clientdevices.auth.certificate.CertificateStore;
import com.aws.greengrass.config.Topic;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.dependency.Context;
import com.aws.greengrass.clientdevices.auth.ClientDevicesAuthService;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.mqtt.bridge.auth.MQTTClientKeyStore;
import com.aws.greengrass.mqtt.bridge.clients.IoTCoreClient;
import com.aws.greengrass.mqtt.bridge.clients.LocalMqttClientFactory;
import com.aws.greengrass.mqtt.bridge.clients.MessageClient;
import com.aws.greengrass.mqtt.bridge.clients.PubSubClient;
import com.aws.greengrass.mqtt.bridge.model.BridgeConfigReference;
import com.aws.greengrass.mqtt.bridge.model.Message;
import com.aws.greengrass.mqtt.bridge.model.MqttMessage;
import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import com.aws.greengrass.testcommons.testutilities.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class MQTTBridgeTest extends GGServiceTestUtil {

    private final ExecutorService executor = TestUtils.synchronousExecutorService();

    @AfterEach
    void cleanup() {
        executor.shutdownNow();
    }

    @Test
    void GIVEN_mqtt_bridge_WHEN_cda_ca_conf_changed_THEN_bridge_keystore_updated(ExtensionContext extensionContext) throws Exception {
        ignoreExceptionOfType(extensionContext, InterruptedException.class);
        serviceFullName = MQTTBridge.SERVICE_NAME;
        initializeMockedConfig();
        TopicMapping mockTopicMapping = mock(TopicMapping.class);
        MessageBridge mockMessageBridge = mock(MessageBridge.class);
        PubSubIPCEventStreamAgent mockPubSubIPCAgent = mock(PubSubIPCEventStreamAgent.class);
        Kernel mockKernel = mock(Kernel.class);
        MQTTClientKeyStore mockMqttClientKeyStore = mock(MQTTClientKeyStore.class);
        MQTTBridge mqttBridge;
        LocalMqttClientFactory localMqttClientFactory = new FakeMqttClientFactory();

        try (Context context = new Context()) {
            Topics config = Topics.of(context, CONFIGURATION_CONFIG_KEY, null);
            config.lookup(CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_BROKER_URI)
                    .dflt("tcp://localhost:8883");
            config.lookup(CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_CLIENT_ID)
                    .dflt("clientId");

            try (MqttClient mockIotMqttClient = mock(MqttClient.class)) {
                mqttBridge =
                        new MQTTBridge(config, mockTopicMapping, mockMessageBridge, mockPubSubIPCAgent, mockIotMqttClient,
                                mockKernel, mockMqttClientKeyStore, localMqttClientFactory, executor, new BridgeConfigReference());
            }

            ClientDevicesAuthService mockClientAuthService = mock(ClientDevicesAuthService.class);
            when(mockKernel.locate(ClientDevicesAuthService.CLIENT_DEVICES_AUTH_SERVICE_NAME))
                    .thenReturn(mockClientAuthService);
            Topics mockClientAuthConfig = Topics.of(context, "runtime", null);
            when(mockClientAuthService.getRuntimeConfig()).thenReturn(mockClientAuthConfig);

            mockClientAuthConfig
                    .lookup(ClientDevicesAuthService.CERTIFICATES_KEY, ClientDevicesAuthService.AUTHORITIES_TOPIC)
                    .withValue(Arrays.asList(
                            CertificateHelper.toPem(
                                    CertificateHelper.createCACertificate(
                                            CertificateStore.newRSAKeyPair(2048),
                                            Date.from(Instant.now()),
                                            Date.from(Instant.now().plusSeconds(100)),
                                            "CA1")),
                            CertificateHelper.toPem(
                                    CertificateHelper.createCACertificate(
                                            CertificateStore.newRSAKeyPair(2048),
                                            Date.from(Instant.now()),
                                            Date.from(Instant.now().plusSeconds(100)),
                                            "CA2"))));
            context.waitForPublishQueueToClear();

            mqttBridge.install();
            mqttBridge.startup();
            mqttBridge.shutdown();
            ArgumentCaptor<List<String>> caListCaptor = ArgumentCaptor.forClass(List.class);
            verify(mockMqttClientKeyStore).updateCA(caListCaptor.capture());
            assertThat(caListCaptor.getValue().size(), is(2));

            // test invalid cda ca config
            mockClientAuthConfig
                    .lookup(ClientDevicesAuthService.CERTIFICATES_KEY, ClientDevicesAuthService.AUTHORITIES_TOPIC)
                    .withValue("invalid");
            context.waitForPublishQueueToClear();

            reset(mockMqttClientKeyStore);
            mqttBridge.install();
            mqttBridge.startup();
            mqttBridge.shutdown();
            verify(mockMqttClientKeyStore, never()).updateCA(caListCaptor.capture());

            // ensure keystore not updated after shutdown
            reset(mockMqttClientKeyStore);
            mockClientAuthConfig
                    .lookup(ClientDevicesAuthService.CERTIFICATES_KEY, ClientDevicesAuthService.AUTHORITIES_TOPIC)
                    .withValue(Arrays.asList(
                            CertificateHelper.toPem(
                                    CertificateHelper.createCACertificate(
                                            CertificateStore.newRSAKeyPair(2048),
                                            Date.from(Instant.now()),
                                            Date.from(Instant.now().plusSeconds(100)),
                                            "CA1")),
                            CertificateHelper.toPem(
                                    CertificateHelper.createCACertificate(
                                            CertificateStore.newRSAKeyPair(2048),
                                            Date.from(Instant.now()),
                                            Date.from(Instant.now().plusSeconds(100)),
                                            "CA2"))));
            context.waitForPublishQueueToClear();
            verify(mockMqttClientKeyStore, never()).updateCA(caListCaptor.capture());
        }
    }

    @Test
    void GIVEN_mqtt_bridge_WHEN_cda_ca_conf_changed_after_shutdown_THEN_bridge_keystore_not_updated(ExtensionContext extensionContext) throws Exception {
        ignoreExceptionOfType(extensionContext, InterruptedException.class);
        serviceFullName = MQTTBridge.SERVICE_NAME;
        initializeMockedConfig();
        TopicMapping mockTopicMapping = mock(TopicMapping.class);
        MessageBridge mockMessageBridge = mock(MessageBridge.class);
        PubSubIPCEventStreamAgent mockPubSubIPCAgent = mock(PubSubIPCEventStreamAgent.class);
        Kernel mockKernel = mock(Kernel.class);
        MQTTClientKeyStore mockMqttClientKeyStore = mock(MQTTClientKeyStore.class);
        MQTTBridge mqttBridge;
        LocalMqttClientFactory localMqttClientFactory = new FakeMqttClientFactory();

        try (Context context = new Context()) {
            Topics config = Topics.of(context, CONFIGURATION_CONFIG_KEY, null);
            config.lookup(CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_BROKER_URI)
                    .dflt("tcp://localhost:8883");
            config.lookup(CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_CLIENT_ID)
                    .dflt("clientId");

            try (MqttClient mockIotMqttClient = mock(MqttClient.class)) {
                mqttBridge =
                        new MQTTBridge(config, mockTopicMapping, mockMessageBridge, mockPubSubIPCAgent, mockIotMqttClient,
                                mockKernel, mockMqttClientKeyStore, localMqttClientFactory, executor, new BridgeConfigReference());
            }

            ClientDevicesAuthService mockClientAuthService = mock(ClientDevicesAuthService.class);
            when(mockKernel.locate(ClientDevicesAuthService.CLIENT_DEVICES_AUTH_SERVICE_NAME))
                    .thenReturn(mockClientAuthService);
            Topics mockClientAuthConfig = Topics.of(context, "runtime", null);
            when(mockClientAuthService.getRuntimeConfig()).thenReturn(mockClientAuthConfig);

            mqttBridge.install();
            mqttBridge.startup();
            mqttBridge.shutdown();

            // ensure keystore not updated after shutdown
            mockClientAuthConfig
                    .lookup(ClientDevicesAuthService.CERTIFICATES_KEY, ClientDevicesAuthService.AUTHORITIES_TOPIC)
                    .withValue(Arrays.asList("CA1", "CA2"));
            context.waitForPublishQueueToClear();
            ArgumentCaptor<List<String>> caListCaptor = ArgumentCaptor.forClass(List.class);
            verify(mockMqttClientKeyStore, never()).updateCA(caListCaptor.capture());
        }
    }

    @Test
    void GIVEN_Greengrass_with_mqtt_bridge_WHEN_Startup_THEN_Mqtt_subscriptions_updated_for_all_clients(ExtensionContext extensionContext) throws Exception {
        ignoreExceptionOfType(extensionContext, InterruptedException.class);
        serviceFullName = MQTTBridge.SERVICE_NAME;
        initializeMockedConfig();
        TopicMapping mockTopicMapping = mock(TopicMapping.class);
        MessageBridge mockMessageBridge = mock(MessageBridge.class);
        Kernel mockKernel = mock(Kernel.class);
        MQTTClientKeyStore mockMqttClientKeyStore = mock(MQTTClientKeyStore.class);
        LocalMqttClientFactory localMqttClientFactory = new FakeMqttClientFactory();

        Topics config = Topics.of(context, CONFIGURATION_CONFIG_KEY, null);
        config.lookup(CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_BROKER_URI)
                .dflt("tcp://localhost:8883");
        config.lookup(CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_CLIENT_ID)
                .dflt("clientId");

        MQTTBridge mqttBridge =
                new MQTTBridge(config, mockTopicMapping, mockMessageBridge, mock(PubSubIPCEventStreamAgent.class),
                        mock(MqttClient.class), mockKernel, mockMqttClientKeyStore, localMqttClientFactory, executor, new BridgeConfigReference());

        ClientDevicesAuthService mockClientAuthService = mock(ClientDevicesAuthService.class);
        when(mockKernel.locate(ClientDevicesAuthService.CLIENT_DEVICES_AUTH_SERVICE_NAME))
                .thenReturn(mockClientAuthService);
        Topics mockClientAuthConfig = mock(Topics.class);
        when(mockClientAuthService.getRuntimeConfig()).thenReturn(mockClientAuthConfig);

        Topic caTopic = Topic.of(context, "authorities", Arrays.asList("CA1", "CA2"));
        when(mockClientAuthConfig
                .lookup(ClientDevicesAuthService.CERTIFICATES_KEY,
                        ClientDevicesAuthService.AUTHORITIES_TOPIC)).thenReturn(caTopic);

        mqttBridge.install();
        // we need to set the messageBridge to the mock since a new messageBridge is created during install()
        mqttBridge.setMessageBridge(mockMessageBridge);
        mqttBridge.startup();
        mqttBridge.shutdown();

        verify(mockMessageBridge).addOrReplaceMessageClientAndUpdateSubscriptions(
                eq(TopicMapping.TopicType.LocalMqtt), any(MessageClient.class));
        verify(mockMessageBridge).addOrReplaceMessageClientAndUpdateSubscriptions(
                eq(TopicMapping.TopicType.Pubsub), any(PubSubClient.class));
        verify(mockMessageBridge).addOrReplaceMessageClientAndUpdateSubscriptions(
                eq(TopicMapping.TopicType.IotCore), any(IoTCoreClient.class));
    }

    // TODO test local client recreated when keystore updated

    static class FakeMqttClientFactory extends LocalMqttClientFactory {
        public FakeMqttClientFactory() {
            super(null, null, null, null);
        }

        @Override
        public MessageClient<MqttMessage> createLocalMqttClient()  {
            return new MessageClient<MqttMessage>() {
                @Override
                public void publish(MqttMessage message) {
                }

                @Override
                public void updateSubscriptions(Set<String> topics, Consumer<MqttMessage> messageHandler) {
                }

                @Override
                public void start() {
                }

                @Override
                public void stop() {
                }

                @Override
                public MqttMessage convertMessage(Message message) {
                    return (MqttMessage) message.toMqtt();
                }
            };
        }
    }
}
