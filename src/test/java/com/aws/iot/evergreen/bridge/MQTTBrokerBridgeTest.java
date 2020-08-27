package com.aws.iot.evergreen.bridge;

import com.aws.iot.evergreen.dependency.State;
import com.aws.iot.evergreen.testcommons.testutilities.EGExtension;
import com.aws.iot.evergreen.testcommons.testutilities.EGServiceTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, EGExtension.class})
public class MQTTBrokerBridgeTest extends EGServiceTestUtil {

    @BeforeEach
    public void setup() {
        serviceFullName = "aws.greengrass.mqtt.broker.bridge";
        initializeMockedConfig();
        when(stateTopic.getOnce()).thenReturn(State.INSTALLED);
    }

    @Test
    public void GIVEN_MQTTBrokerBridge_WHEN_started_THEN_does_not_throw() {
        MQTTBrokerBridge mqttBrokerBridge = new MQTTBrokerBridge(config);
        mqttBrokerBridge.postInject();
        mqttBrokerBridge.startup();
        mqttBrokerBridge.shutdown();
    }
}
