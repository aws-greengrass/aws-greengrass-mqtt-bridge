/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge;

import com.aws.greengrass.builtin.services.pubsub.PubSubIPCEventStreamAgent;
import com.aws.greengrass.certificatemanager.certificate.CsrProcessingException;
import com.aws.greengrass.componentmanager.KernelConfigResolver;
import com.aws.greengrass.config.Node;
import com.aws.greengrass.config.Subscriber;
import com.aws.greengrass.config.Topic;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.config.WhatHappened;
import com.aws.greengrass.dependency.ImplementsService;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.device.ClientDevicesAuthService;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.lifecyclemanager.PluginService;
import com.aws.greengrass.lifecyclemanager.exceptions.ServiceLoadException;
import com.aws.greengrass.mqttbridge.auth.CsrGeneratingException;
import com.aws.greengrass.mqttbridge.auth.MQTTClientKeyStore;
import com.aws.greengrass.mqttbridge.clients.IoTCoreClient;
import com.aws.greengrass.mqttbridge.clients.MQTTClient;
import com.aws.greengrass.mqttbridge.clients.MQTTClientException;
import com.aws.greengrass.mqttbridge.clients.PubSubClient;
import com.aws.greengrass.mqttbridge.util.BatchedSubscriber;
import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.util.Utils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyStoreException;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.inject.Inject;

@ImplementsService(name = MQTTBridge.SERVICE_NAME)
public class MQTTBridge extends PluginService {
    public static final String SERVICE_NAME = "aws.greengrass.clientdevices.mqtt.Bridge";

    @Getter(AccessLevel.PACKAGE) // Getter for unit tests
    private final TopicMapping topicMapping;
    private final MessageBridge messageBridge;
    private final Kernel kernel;
    private final MQTTClientKeyStore mqttClientKeyStore;
    @Setter(AccessLevel.PACKAGE) // Setter for unit tests
    private MQTTClientFactory mqttClientFactory;
    private final ConfigurationChangeHandler configurationChangeHandler;
    private MQTTClient mqttClient;
    private PubSubClient pubSubClient;
    private IoTCoreClient ioTCoreClient;
    private boolean isSSL;
    private URI brokerUri;
    private String clientId;
    private Topic certificateAuthoritiesTopic;
    private Subscriber certificateAuthoritiesTopicSubscriber;

    private static final JsonMapper OBJECT_MAPPER =
            JsonMapper.builder().enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES).build();

    @FunctionalInterface
    interface MQTTClientFactory {
        MQTTClient get() throws MQTTClientException;
    }

    /**
     * Ctr for MQTTBridge.
     *
     * @param topics             topics passed by the Nucleus
     * @param topicMapping       mapping of mqtt topics to iotCore/pubsub topics
     * @param pubSubIPCAgent     IPC agent for pubsub
     * @param iotMqttClient      mqtt client for iot core
     * @param kernel             Greengrass kernel
     * @param mqttClientKeyStore KeyStore for MQTT Client
     * @param executorService    Executor service
     */
    @Inject
    public MQTTBridge(Topics topics, TopicMapping topicMapping, PubSubIPCEventStreamAgent pubSubIPCAgent,
                      MqttClient iotMqttClient, Kernel kernel, MQTTClientKeyStore mqttClientKeyStore,
                      ExecutorService executorService) {
        this(topics, topicMapping, new MessageBridge(topicMapping), pubSubIPCAgent, iotMqttClient, kernel,
                mqttClientKeyStore, executorService);
    }

    protected MQTTBridge(Topics topics, TopicMapping topicMapping, MessageBridge messageBridge,
                         PubSubIPCEventStreamAgent pubSubIPCAgent, MqttClient iotMqttClient, Kernel kernel,
                         MQTTClientKeyStore mqttClientKeyStore, ExecutorService executorService) {
        super(topics);
        this.topicMapping = topicMapping;
        this.kernel = kernel;
        this.mqttClientKeyStore = mqttClientKeyStore;
        this.messageBridge = messageBridge;
        this.pubSubClient = new PubSubClient(pubSubIPCAgent);
        this.ioTCoreClient = new IoTCoreClient(iotMqttClient);
        this.mqttClientFactory = () -> new MQTTClient(brokerUri, clientId, mqttClientKeyStore, executorService);
        this.configurationChangeHandler = new ConfigurationChangeHandler();
    }

    @Override
    public void install() {
        configurationChangeHandler.listen();

        try {
            this.brokerUri = BridgeConfig.getBrokerUri(config);
        } catch (URISyntaxException e) {
            serviceErrored(e);
            return;
        }
        this.isSSL = "ssl".equalsIgnoreCase(brokerUri.getScheme());
        this.clientId = BridgeConfig.getClientId(config);
    }

    @Override
    public void startup() {
        if (isSSL) {
            try {
                mqttClientKeyStore.init();
            } catch (CsrProcessingException | KeyStoreException | CsrGeneratingException e) {
                serviceErrored(e);
                return;
            }

            try {
                subscribeToCertificateAuthoritiesTopic(caPemList -> {
                    try {
                        if (!Utils.isEmpty(caPemList)) {
                            logger.atDebug().kv("numCaCerts", caPemList.size()).log("CA update received");
                            mqttClientKeyStore.updateCA(caPemList);
                        }
                    } catch (IOException | CertificateException | KeyStoreException e) {
                        serviceErrored(e);
                    }
                });
            } catch (ServiceLoadException e) {
                serviceErrored(e);
                return;
            }
        }

        try {
            mqttClient = mqttClientFactory.get();
            mqttClient.start();
            messageBridge.addOrReplaceMessageClient(TopicMapping.TopicType.LocalMqtt, mqttClient);
        } catch (MQTTClientException e) {
            serviceErrored(e);
            return;
        }
        pubSubClient.start();
        messageBridge.addOrReplaceMessageClient(TopicMapping.TopicType.Pubsub, pubSubClient);

        ioTCoreClient.start();
        messageBridge.addOrReplaceMessageClient(TopicMapping.TopicType.IotCore, ioTCoreClient);

        messageBridge.start();

        reportState(State.RUNNING);
    }

    @Override
    public void shutdown() {
        unsubscribeFromCertificateAuthoritiesTopic();

        messageBridge.stop();
        messageBridge.removeAllMessageClients();

        if (mqttClient != null) {
            mqttClient.stop();
        }

        if (pubSubClient != null) {
            pubSubClient.stop();
        }

        if (ioTCoreClient != null) {
            ioTCoreClient.stop();
        }
    }

    @SuppressWarnings("unchecked")
    private void subscribeToCertificateAuthoritiesTopic(Consumer<List<String>> onCAChange) throws ServiceLoadException {
        certificateAuthoritiesTopic = kernel
                .locate(ClientDevicesAuthService.CLIENT_DEVICES_AUTH_SERVICE_NAME).getConfig()
                .lookup(RUNTIME_STORE_NAMESPACE_TOPIC,
                        ClientDevicesAuthService.CERTIFICATES_KEY,
                        ClientDevicesAuthService.AUTHORITIES_TOPIC);

        certificateAuthoritiesTopicSubscriber = (what, caPemList) ->
                onCAChange.accept((List<String>) caPemList.toPOJO());

        certificateAuthoritiesTopic.subscribe(certificateAuthoritiesTopicSubscriber);
    }

    private void unsubscribeFromCertificateAuthoritiesTopic() {
        if (certificateAuthoritiesTopic != null && certificateAuthoritiesTopicSubscriber != null) {
            certificateAuthoritiesTopic.remove(certificateAuthoritiesTopicSubscriber);
        }
    }

    /**
     * Responsible for handling all bridge config changes.
     */
    public class ConfigurationChangeHandler {

        private final Topics configurationTopics = config.lookupTopics(KernelConfigResolver.CONFIGURATION_CONFIG_KEY);
        private final Topics mappingConfigTopics = config.lookupTopics(BridgeConfig.PATH_MQTT_TOPIC_MAPPING);

        private final BatchedSubscriber subscriber = new BatchedSubscriber(configurationTopics, (what, whatChanged) -> {

            if (what == WhatHappened.initialized
                    || whatChanged.stream().allMatch(n -> n.childOf(BridgeConfig.KEY_MQTT_TOPIC_MAPPING))) {

                Map<String, TopicMapping.MappingEntry> mapping;
                try {
                    mapping = getMapping();
                } catch (IllegalArgumentException e) {
                    serviceErrored(e);
                    return;
                }

                logger.atInfo("service-config-change").kv("mapping", mapping).log("Updating mapping");
                topicMapping.updateMapping(mapping);
                return;
            }

            logger.atInfo("service-config-change")
                    .kv("changes", whatChanged.stream().map(Node::getName).collect(Collectors.toList()))
                    .log("Requesting reinstallation of bridge");
            requestReinstall();
        });

        private Map<String, TopicMapping.MappingEntry> getMapping() {
            return OBJECT_MAPPER.convertValue(
                    mappingConfigTopics.toPOJO(),
                    new TypeReference<Map<String, TopicMapping.MappingEntry>>() {
                    }
            );
        }

        /**
         * Begin listening and responding to bridge configuration changes.
         *
         * <p>This operation is idempotent.
         */
        public void listen() {
            subscriber.subscribe();
        }
    }
}
