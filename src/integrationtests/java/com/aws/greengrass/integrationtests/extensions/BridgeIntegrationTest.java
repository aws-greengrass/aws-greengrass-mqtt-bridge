/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests.extensions;


import com.aws.greengrass.mqtt.bridge.model.MqttVersion;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.UniqueRootPathExtension;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Convenience annotation for a bridge integration test method. This will automatically take care of
 * spinning up a local broker and greengrass context as necessary.
 * <pre>
 * class MyTest {
 *     &#64;BridgeIntegrationTest(withConfig = "config.yaml", withBrokers = Broker.MQTT5)
 *     void testPerBrokerWithGreengrassRunning(BridgeIntegrationTestContext context) {
 *         // test
 *     }
 * }
 * </pre>
 *
 * @see BridgeIntegrationTestExtension
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@TestTemplate
@ExtendWith({GGExtension.class, UniqueRootPathExtension.class, MockitoExtension.class, BridgeIntegrationTestExtension.class})
public @interface BridgeIntegrationTest {
    String withConfig() default "";
    Broker[] withBrokers() default {};
    MqttVersion[] withLocalClientVersions() default {};
}
