/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge;

import com.aws.greengrass.builtin.services.pubsub.PubSubIPCEventStreamAgent;
import com.aws.greengrass.certificatemanager.certificate.CsrProcessingException;
import com.aws.greengrass.config.Node;
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
import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.util.Coerce;
import com.aws.greengrass.util.Utils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import lombok.AccessLevel;
import lombok.Getter;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import javax.inject.Inject;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;

@ImplementsService(name = MQTTBridge.SERVICE_NAME)
public class MQTTBridge extends PluginService {
    public static final String SERVICE_NAME = "aws.greengrass.clientdevices.mqtt.Bridge";

    @Getter(AccessLevel.PACKAGE) // Getter for unit tests
    private final TopicMapping topicMapping;
    private final MessageBridge messageBridge;
    private final Kernel kernel;
    private final MQTTClientKeyStore mqttClientKeyStore;
    private final ExecutorService executorService;
    private MQTTClient mqttClient;
    private PubSubClient pubSubClient;
    private IoTCoreClient ioTCoreClient;
    private static final JsonMapper OBJECT_MAPPER =
            JsonMapper.builder().enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES).build();
    static final String MQTT_TOPIC_MAPPING = "mqttTopicMapping";
    static final String BROKER_URI_KEY = "brokerUri";

    /**
     * Ctr for MQTTBridge.
     *
     * @param topics             topics passed by by the Nucleus
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
        this.executorService = executorService;
    }

    @Override
    public void install() {
        this.config.lookupTopics(CONFIGURATION_CONFIG_KEY).subscribe((whatHappened, node) -> {
            // Skip updating MQTT topic mapping if the event is WhatHappened.timestampUpdated
            // or WhatHappened.interiorAdded.
            // Also skip if the event is updating brokerUri.
            if (skipUpdatingMqttTopicMapping(whatHappened, node)) {
                return;
            }
            logger.atDebug().kv("why", whatHappened).kv("node", node).log();
            Topics mappingConfigTopics = this.config.lookupTopics(CONFIGURATION_CONFIG_KEY, MQTT_TOPIC_MAPPING);
            if (mappingConfigTopics.isEmpty()) {
                logger.debug("Mapping empty");
                topicMapping.updateMapping(Collections.emptyMap());
                return;
            }

            try {
                Map<String, TopicMapping.MappingEntry> mapping = OBJECT_MAPPER
                        .convertValue(mappingConfigTopics.toPOJO(),
                                new TypeReference<Map<String, TopicMapping.MappingEntry>>() {
                                });
                logger.atInfo().kv("mapping", mapping).log("Updating mapping");
                topicMapping.updateMapping(mapping);
            } catch (IllegalArgumentException e) {
                logger.atError("Invalid topic mapping").kv("TopicMapping", mappingConfigTopics.toString()).log();
                // Currently, Nucleus spills all exceptions in std err which junit consider failures
                serviceErrored(String.format("Invalid topic mapping. %s", e.getMessage()));
            }
        });
    }

    @Override
    public void startup() {
        try {
            mqttClientKeyStore.init();
        } catch (CsrProcessingException | KeyStoreException | CsrGeneratingException e) {
            serviceErrored(e);
            return;
        }

        try {
            kernel.locate(ClientDevicesAuthService.CLIENT_DEVICES_AUTH_SERVICE_NAME).getConfig()
                    .lookup(RUNTIME_STORE_NAMESPACE_TOPIC, ClientDevicesAuthService.CERTIFICATES_KEY,
                            ClientDevicesAuthService.AUTHORITIES_TOPIC).subscribe((why, newv) -> {
                try {
                    List<String> caPemList = (List<String>) newv.toPOJO();
                    if (Utils.isEmpty(caPemList)) {
                        logger.debug("CA list null or empty");
                        return;
                    }
                    mqttClientKeyStore.updateCA(caPemList);
                } catch (IOException | CertificateException | KeyStoreException e) {
                    logger.atError("Invalid CA list").kv("CAList", Coerce.toString(newv)).log();
                    serviceErrored(String.format("Invalid CA list. %s", e.getMessage()));
                }
            });
        } catch (ServiceLoadException e) {
            logger.atError().cause(e).log("Unable to locate {} service while subscribing to CA certificates",
                    ClientDevicesAuthService.CLIENT_DEVICES_AUTH_SERVICE_NAME);
            serviceErrored(e);
            return;
        }

        try {
            if (mqttClient == null) {
                mqttClient = new MQTTClient(this.config, mqttClientKeyStore, this.executorService);
            }
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

        reportState(State.RUNNING);
    }

    @Override
    public void shutdown() {
        messageBridge.removeMessageClient(TopicMapping.TopicType.LocalMqtt);
        if (mqttClient != null) {
            mqttClient.stop();
        }

        messageBridge.removeMessageClient(TopicMapping.TopicType.Pubsub);
        if (pubSubClient != null) {
            pubSubClient.stop();
        }

        messageBridge.removeMessageClient(TopicMapping.TopicType.IotCore);
        if (ioTCoreClient != null) {
            ioTCoreClient.stop();
        }
    }

    private boolean skipUpdatingMqttTopicMapping(WhatHappened whatHappened, Node node) {
        if (whatHappened == WhatHappened.timestampUpdated || whatHappened == WhatHappened.interiorAdded) {
            return true;
        }
        if (node != null && BROKER_URI_KEY.equals(node.getName())) {
            logger.atTrace().kv("why", whatHappened).kv("node", node)
                    .log("Broker URI update. Skip updating topic mapping");
            return true;
        }
        return false;
    }
}
