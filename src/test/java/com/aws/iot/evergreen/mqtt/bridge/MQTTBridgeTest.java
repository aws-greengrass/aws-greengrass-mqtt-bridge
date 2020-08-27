package com.aws.iot.evergreen.mqtt.bridge;

import com.aws.iot.evergreen.dependency.State;
import com.aws.iot.evergreen.testcommons.testutilities.EGExtension;
import com.aws.iot.evergreen.testcommons.testutilities.EGServiceTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, EGExtension.class})
public class MQTTBridgeTest extends EGServiceTestUtil {

    @BeforeEach
    public void setup() {
        serviceFullName = MQTTBridge.MQTT_BRIDGE_SERVICE_NAME;
        initializeMockedConfig();
        when(stateTopic.getOnce()).thenReturn(State.INSTALLED);
    }

    @Test
    public void GIVEN_MQTTBridge_WHEN_started_THEN_does_not_throw() {
        MQTTBridge mqttBridge = new MQTTBridge(config);
        mqttBridge.postInject();
        mqttBridge.startup();
        mqttBridge.shutdown();
    }
}
