package com.aws.iot.evergreen.mqtt.bridge;

import com.aws.iot.evergreen.builtin.services.pubsub.PubSubIPCAgent;
import com.aws.iot.evergreen.config.Topics;
import com.aws.iot.evergreen.dcm.DCMService;
import com.aws.iot.evergreen.dcm.certificate.CsrProcessingException;
import com.aws.iot.evergreen.dependency.ImplementsService;
import com.aws.iot.evergreen.dependency.State;
import com.aws.iot.evergreen.kernel.EvergreenService;
import com.aws.iot.evergreen.kernel.Kernel;
import com.aws.iot.evergreen.kernel.exceptions.ServiceLoadException;
import com.aws.iot.evergreen.mqtt.MqttClient;
import com.aws.iot.evergreen.mqtt.bridge.auth.CsrGeneratingException;
import com.aws.iot.evergreen.mqtt.bridge.auth.MQTTClientKeyStore;
import com.aws.iot.evergreen.mqtt.bridge.clients.IoTCoreClient;
import com.aws.iot.evergreen.mqtt.bridge.clients.MQTTClient;
import com.aws.iot.evergreen.mqtt.bridge.clients.MQTTClientException;
import com.aws.iot.evergreen.mqtt.bridge.clients.PubSubClient;
import com.aws.iot.evergreen.packagemanager.KernelConfigResolver;
import com.aws.iot.evergreen.util.Coerce;
import com.aws.iot.evergreen.util.Utils;
import lombok.AccessLevel;
import lombok.Getter;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.cert.CertificateException;
import java.util.List;
import javax.inject.Inject;

@ImplementsService(name = MQTTBridge.SERVICE_NAME)
public class MQTTBridge extends EvergreenService {
    public static final String SERVICE_NAME = "aws.greengrass.mqtt.bridge";
    static final String RUNTIME_CONFIG_KEY = "runtime";
    static final String CERTIFICATES_TOPIC = "certificates";
    static final String AUTHORITIES = "authorities";

    @Getter(AccessLevel.PACKAGE) // Getter for unit tests
    private final TopicMapping topicMapping;
    private final MessageBridge messageBridge;
    private final Kernel kernel;
    private final MQTTClientKeyStore mqttClientKeyStore;
    private MQTTClient mqttClient;
    private PubSubClient pubSubClient;
    private IoTCoreClient ioTCoreClient;
    static final String MQTT_TOPIC_MAPPING = "mqttTopicMapping";

    /**
     * Ctr for MQTTBridge.
     *
     * @param topics             topics passed by by the kernel
     * @param topicMapping       mapping of mqtt topics to iotCore/pubsub topics
     * @param pubSubIPCAgent     IPC agent for pubsub
     * @param iotMqttClient      mqtt client for iot core
     * @param kernel             greengrass kernel
     * @param mqttClientKeyStore KeyStore for MQTT Client
     */
    @Inject
    public MQTTBridge(Topics topics, TopicMapping topicMapping, PubSubIPCAgent pubSubIPCAgent, MqttClient iotMqttClient,
                      Kernel kernel, MQTTClientKeyStore mqttClientKeyStore) {
        this(topics, topicMapping, new MessageBridge(topicMapping), pubSubIPCAgent, iotMqttClient, kernel,
             mqttClientKeyStore);
    }

    protected MQTTBridge(Topics topics, TopicMapping topicMapping, MessageBridge messageBridge, 
                         PubSubIPCAgent pubSubIPCAgent, MqttClient iotMqttClient, Kernel kernel,
                         MQTTClientKeyStore mqttClientKeyStore) {
        super(topics);
        this.topicMapping = topicMapping;
        this.kernel = kernel;
        this.mqttClientKeyStore = mqttClientKeyStore;
        this.messageBridge = messageBridge;
        this.pubSubClient = new PubSubClient(pubSubIPCAgent);
        this.ioTCoreClient = new IoTCoreClient(iotMqttClient);
    }

    @Override
    public void install() {
        this.config.lookup(KernelConfigResolver.PARAMETERS_CONFIG_KEY, MQTT_TOPIC_MAPPING).dflt("[]")
                .subscribe((why, newv) -> {
                    try {
                        String mapping = Coerce.toString(newv);
                        if (Utils.isEmpty(mapping)) {
                            logger.debug("Mapping null or empty");
                            return;
                        }
                        logger.atDebug().kv("mapping", mapping).log("Updating mapping");
                        topicMapping.updateMapping(mapping);
                    } catch (IOException e) {
                        logger.atError("Invalid topic mapping").kv("TopicMapping", Coerce.toString(newv)).log();
                        // Currently, kernel spills all exceptions in std err which junit consider failures
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
        }

        try {
            kernel.locate(DCMService.DCM_SERVICE_NAME).getConfig()
                    .lookup(RUNTIME_CONFIG_KEY, CERTIFICATES_TOPIC, AUTHORITIES)
                    .subscribe((why, newv) -> {
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
                    DCMService.DCM_SERVICE_NAME);
            serviceErrored(e);
        }

        try {
            mqttClient = new MQTTClient(this.config, mqttClientKeyStore);
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
}
