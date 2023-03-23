/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.clients;

import lombok.Getter;
import software.amazon.awssdk.crt.mqtt5.Mqtt5Client;

import static org.mockito.Mockito.mock;


public class MockMqtt5Client {

    @Getter
    private final Mqtt5Client client = mock(Mqtt5Client.class);

}
