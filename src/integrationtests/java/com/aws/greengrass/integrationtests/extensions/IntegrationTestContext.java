/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests.extensions;

import com.aws.greengrass.lifecyclemanager.Kernel;
import lombok.Data;

import java.nio.file.Path;

@Data
public class IntegrationTestContext {
    Broker broker;
    Integer brokerPort;
    String brokerHost;
    Path rootDir;
    Kernel kernel;
}
