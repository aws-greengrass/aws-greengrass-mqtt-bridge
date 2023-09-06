/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests.extensions;


import org.junit.jupiter.api.Tag;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Convenience annotation for bridge performance test class.
 *
 * <p>Example: bridge performance test class, with test method that exercises mqtt3 and mqtt5 brokers.
 *
 * <pre>
 * &#64;BridgePerformanceTest
 * class MyTest {
 *     &#64;TestWithAllBrokers
 *     &#64;WithKernel("config.yaml")
 *     void testPerBrokerWithGreengrassRunning(Broker broker) {
 *         // test
 *     }
 * }
 * </pre>
 *
 * @see BridgeIntegrationTestExtension
 * @see WithKernel
 * @see TestWithAllBrokers
 * @see TestWithMqtt3Broker
 * @see TestWithMqtt5Broker
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Tag("performance")
@BridgeIntegrationTestExtensions
public @interface BridgePerformanceTest {
}
