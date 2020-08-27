package com.aws.iot.evergreen.mqtt.bridge;

import com.aws.iot.evergreen.config.Topics;
import com.aws.iot.evergreen.dependency.ImplementsService;
import com.aws.iot.evergreen.dependency.State;
import com.aws.iot.evergreen.kernel.EvergreenService;

import javax.inject.Inject;

@ImplementsService(name = MQTTBridge.MQTT_BRIDGE_SERVICE_NAME)
public class MQTTBridge extends EvergreenService {
    public static final String MQTT_BRIDGE_SERVICE_NAME = "aws.greengrass.mqtt.bridge";

    @Inject
    public MQTTBridge(Topics topics) {
        super(topics);
    }

    @Override
    public void startup() {
        reportState(State.RUNNING);
    }

    @Override
    public void shutdown() {
    }

}
