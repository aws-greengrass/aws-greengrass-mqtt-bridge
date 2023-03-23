/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.clients;

import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.TestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.URI;
import java.util.concurrent.ExecutorService;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class LocalMqtt5ClientTest {

    ExecutorService executorService = TestUtils.synchronousExecutorService();

    MockMqtt5Client mockMqtt5Client = new MockMqtt5Client();

    LocalMqtt5Client client = new LocalMqtt5Client(
            URI.create("tcp://localhost:1883"),
            "test-client",
            executorService,
            mockMqtt5Client.getClient()
    );

    @Test
    void TODO() {
    }
}
