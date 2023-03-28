/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Mqtt5RouteOptions {

    public static final boolean DEFAULT_NO_LOCAL = false;
    public static final boolean DEFAULT_RETAIN_AS_PUBLISHED = true;

    @Builder.Default
    boolean noLocal = DEFAULT_NO_LOCAL;
    @Builder.Default
    boolean retainAsPublished = DEFAULT_RETAIN_AS_PUBLISHED;
}
