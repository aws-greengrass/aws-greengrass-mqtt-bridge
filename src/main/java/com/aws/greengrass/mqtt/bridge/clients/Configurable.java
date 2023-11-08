/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.clients;

import com.aws.greengrass.mqtt.bridge.BridgeConfig;

@FunctionalInterface
public interface Configurable {
    void applyConfig(BridgeConfig config);
}
