/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.model;

import com.aws.greengrass.mqtt.bridge.BridgeConfig;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Convenience wrapper for BridgeConfig to make it injectable in other classes.
 * Value of this reference is managed by {@link com.aws.greengrass.mqtt.bridge.MQTTBridge}.
 */
public class BridgeConfigReference extends AtomicReference<BridgeConfig> {
    private static final long serialVersionUID = 4845695073418707546L;
}
