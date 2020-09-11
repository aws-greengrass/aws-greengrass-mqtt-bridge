package com.aws.iot.evergreen.mqtt.bridge;

import com.aws.iot.evergreen.config.Topics;
import com.aws.iot.evergreen.dcm.DCMService;
import com.aws.iot.evergreen.dcm.certificate.CsrProcessingException;
import com.aws.iot.evergreen.dependency.ImplementsService;
import com.aws.iot.evergreen.dependency.State;
import com.aws.iot.evergreen.kernel.EvergreenService;
import com.aws.iot.evergreen.kernel.Kernel;
import com.aws.iot.evergreen.kernel.exceptions.ServiceLoadException;
import com.aws.iot.evergreen.mqtt.bridge.auth.CsrGeneratingException;
import com.aws.iot.evergreen.mqtt.bridge.auth.MQTTClientKeyStore;
import com.aws.iot.evergreen.mqtt.bridge.clients.MQTTClient;
import com.aws.iot.evergreen.mqtt.bridge.clients.MQTTClientException;
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
    private MQTTClient mqttClient;
    static final String MQTT_TOPIC_MAPPING = "mqttTopicMapping";

    /**
     * Ctr for MQTTBridge.
     *
     * @param topics             topics passed by by the kernel
     * @param topicMapping       mapping of mqtt topics to iotCore/pubsub topics
     * @param kernel             greengrass kernel
     * @param mqttClientKeyStore KeyStore for MQTT Client
     */
    @Inject
    public MQTTBridge(Topics topics, TopicMapping topicMapping, Kernel kernel, MQTTClientKeyStore mqttClientKeyStore) {
        this(topics, topicMapping, new MessageBridge(topicMapping), kernel, mqttClientKeyStore);
    }

    protected MQTTBridge(Topics topics, TopicMapping topicMapping, MessageBridge messageBridge, Kernel kernel,
                         MQTTClientKeyStore mqttClientKeyStore) {
        super(topics);
        this.topicMapping = topicMapping;

        topics.lookup(KernelConfigResolver.PARAMETERS_CONFIG_KEY, MQTT_TOPIC_MAPPING).dflt("[]")
                .subscribe((why, newv) -> {
                    try {
                        String mapping = Coerce.toString(newv);
                        if (Utils.isEmpty(mapping)) {
                            logger.debug("Mapping null or empty");
                            return;
                        }
                        topicMapping.updateMapping(mapping);
                    } catch (IOException e) {
                        logger.atError("Invalid topic mapping").kv("TopicMapping", Coerce.toString(newv)).log();
                        // Currently, kernel spills all exceptions in std err which junit consider failures
                        serviceErrored(String.format("Invalid topic mapping. %s", e.getMessage()));
                    }
                });

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

        this.messageBridge = messageBridge;
        try {
            this.mqttClient = new MQTTClient(topics, mqttClientKeyStore);
        } catch (MQTTClientException e) {
            serviceErrored(e);
        }
    }

    @Override
    public void startup() {
        try {
            mqttClient.start();
            messageBridge.addOrReplaceMessageClient(TopicMapping.TopicType.LocalMqtt, mqttClient);
        } catch (MQTTClientException e) {
            serviceErrored(e);
            return;
        }
        reportState(State.RUNNING);
    }

    @Override
    public void shutdown() {
        messageBridge.removeMessageClient(TopicMapping.TopicType.LocalMqtt);
        if (mqttClient != null) {
            mqttClient.stop();
        }
    }
}
