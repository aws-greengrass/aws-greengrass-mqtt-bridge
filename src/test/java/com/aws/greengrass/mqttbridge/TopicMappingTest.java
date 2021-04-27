/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge;

import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.core.JsonParseException;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class TopicMappingTest {

    @Test
    void GIVEN_mapping_as_json_string_WHEN_updateMapping_THEN_mapping_updated_successfully() throws Exception {
        TopicMapping mapping = new TopicMapping();
        CountDownLatch updateLatch = new CountDownLatch(1);
        mapping.listenToUpdates(updateLatch::countDown);
        mapping.updateMapping(
                "{\n" + "  \"m1\" :{\"topic\": \"mqtt/topic\", \"source\": \"LocalMqtt\",  \"target\": \"IotCore\"},\n"
                        + "  \"m2\" :{\"topic\": \"mqtt/topic2\", \"source\": \"LocalMqtt\",  \"target\": \"Pubsub\"},\n"
                        + "  \"m3\" :{\"topic\": \"mqtt/topic3\", \"source\": \"LocalMqtt\",  \"target\": \"IotCore\"}\n"
                        + "}");

        Assertions.assertTrue(updateLatch.await(100, TimeUnit.MILLISECONDS));

        Map<String, TopicMapping.MappingEntry> expectedMapping = new HashMap<>();
        expectedMapping.put("m1", new TopicMapping.MappingEntry("mqtt/topic", TopicMapping.TopicType.LocalMqtt,
                TopicMapping.TopicType.IotCore));
        expectedMapping.put("m2", new TopicMapping.MappingEntry("mqtt/topic2", TopicMapping.TopicType.LocalMqtt,
                TopicMapping.TopicType.Pubsub));
        expectedMapping.put("m3", new TopicMapping.MappingEntry("mqtt/topic3", TopicMapping.TopicType.LocalMqtt,
                TopicMapping.TopicType.IotCore));

        assertEquals(mapping.getMapping(), expectedMapping);
    }

    @Test
    void GIVEN_invalid_mapping_as_json_string_WHEN_updateMapping_THEN_mapping_not_updated() throws Exception {
        TopicMapping mapping = new TopicMapping();
        CountDownLatch updateLatch = new CountDownLatch(1);
        mapping.listenToUpdates(updateLatch::countDown);

        assertThat(mapping.getMapping().size(), is(equalTo(0)));
        // Updating with invalid mapping (Providing type as Pubsub-Invalid)
        Assertions.assertThrows(IOException.class, () -> mapping.updateMapping(
                "{\n" + "  \"m1\" :{\"topic\": \"mqtt/topic\", \"source\": \"LocalMqtt\",  \"target\": \"IotCore\"},\n"
                        + "  \"m2\" :{\"topic\": \"mqtt/topic2\", \"source\": \"LocalMqtt\",  \"target\": \"Pubsub-Invalid\"},\n"
                        + "  \"m3\" :{\"topic\": \"mqtt/topic3\", \"source\": \"LocalMqtt\",  \"target\": \"IotCore\"}\n"
                        + "}"));

        Assertions.assertFalse(updateLatch.await(100, TimeUnit.MILLISECONDS));

        assertThat(mapping.getMapping().size(), is(equalTo(0)));
    }

    @Test
    void GIVEN_null_mapping_as_json_string_WHEN_updateMapping_THEN_NPE_thrown() throws Exception {
        TopicMapping mapping = new TopicMapping();
        assertThat(mapping.getMapping().size(), is(equalTo(0)));
        Assertions.assertThrows(NullPointerException.class, () -> mapping.updateMapping(null));
        assertThat(mapping.getMapping().size(), is(equalTo(0)));
    }

    @Test
    void GIVEN_mapping_with_duplicate_keys_WHEN_updateMapping_THEN_exception_thrown() throws Exception {
        TopicMapping mapping = new TopicMapping();
        CountDownLatch updateLatch = new CountDownLatch(1);
        mapping.listenToUpdates(updateLatch::countDown);
        Assertions.assertThrows(JsonParseException.class, () -> mapping.updateMapping(
                "{\n" + "  \"m1\" :{\"topic\": \"mqtt/topic\", \"source\": \"LocalMqtt\",  \"target\": \"IotCore\"},\n"
                        + "  \"m2\" :{\"topic\": \"mqtt/topic2\", \"source\": \"LocalMqtt\",  \"target\": \"Pubsub\"},\n"
                        + "  \"m2\" :{\"topic\": \"mqtt/topic3\", \"source\": \"LocalMqtt\",  \"target\": \"IotCore\"}\n"
                        + "}"));
    }
}
