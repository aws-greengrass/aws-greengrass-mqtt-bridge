/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests.extensions;


import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.UniqueRootPathExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Convenience annotation for integration test class.
 *
 * <p>Example: integration test class, with test method that exercises mqtt3 and mqtt5 brokers.
 *
 * <pre>
 * &#64;IntegrationTest
 * class MyTest {
 *     &#64;TestWithAllBrokers
 *     &#64;WithKernel("config.yaml")
 *     void testPerBrokerWithGreengrassRunning(Broker broker) {
 *         // test
 *     }
 * }
 *
 * </pre>
 *
 * @see IntegrationTestExtension
 * @see WithKernel
 * @see TestWithAllBrokers
 * @see TestWithMqtt3Broker
 * @see TestWithMqtt5Broker
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith({GGExtension.class, UniqueRootPathExtension.class, MockitoExtension.class, IntegrationTestExtension.class})
public @interface IntegrationTest {
}
