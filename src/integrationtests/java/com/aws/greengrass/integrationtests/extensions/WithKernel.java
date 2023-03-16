/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests.extensions;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation tells {@link IntegrationTestExtension} to spin
 * up a greengrass Kernel before the test starts.
 *
 * NOTE: the resource defined must be present in the
 * same package as the test class.
 *
 * <p>NOTE: test class must be annotated with {@link IntegrationTest}.
 *
 * <pre>
 * &#64;WithKernel("config.yaml")
 * void exampleTest() {
 *     // test
 * }
 * </pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface WithKernel {
    String value() default "";
}
