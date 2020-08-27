package com.aws.iot.evergreen.bridge;

import com.aws.iot.evergreen.config.Topics;
import com.aws.iot.evergreen.dependency.ImplementsService;
import com.aws.iot.evergreen.dependency.State;
import com.aws.iot.evergreen.kernel.EvergreenService;

import javax.inject.Inject;
import javax.inject.Singleton;

@ImplementsService(name = MQTTBrokerBridge.MQTT_BROKER_BRIDGE_SERVICE_NAME)
@Singleton
public class MQTTBrokerBridge extends EvergreenService {
    public static final String MQTT_BROKER_BRIDGE_SERVICE_NAME = "aws.greengrass.mqtt.broker.bridge";

    @Inject
    public MQTTBrokerBridge(Topics topics) {
        super(topics);
    }

    @Override
    public void startup() {
        reportState(State.RUNNING);
    }

    @Override
    public void shutdown() {
        logger.atInfo().log("MQTTBrokerBridge is shutting down!");
    }

}
