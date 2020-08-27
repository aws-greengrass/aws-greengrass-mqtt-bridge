/* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0 */

package com.aws.iot.evergreen.mqtt.bridge;

import com.aws.iot.evergreen.testcommons.testutilities.EGExtension;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

@ExtendWith({MockitoExtension.class, EGExtension.class})
public class TopicMappingTest {

    @Test
    void GIVEN_mapping_as_json_string_WHEN_updateMapping_THEN_mapping_updated_successfully() throws Exception {
        TopicMapping mapping = new TopicMapping();
        mapping.updateMapping("[\n"
                + "  {\"MqttTopic\": \"mqtt/topic\", \"MappedTopic\": \"/test/cloud/topic\", \"Type\": \"IotCore\"},\n"
                + "  {\"MqttTopic\": \"mqtt/topic2\", \"MappedTopic\": \"/test/pubsub/topic\", \"Type\": \"Pubsub\"},\n"
                + "  {\"MqttTopic\": \"mqtt/topic3\", \"MappedTopic\": \"/test/cloud/topic2\", \"Type\": \"IotCore\"}\n"
                + "]");
        List<TopicMapping.MappingEntry> expectedMapping = new ArrayList<>();
        expectedMapping.add(new TopicMapping.MappingEntry("mqtt/topic", "/test/cloud/topic",
                TopicMapping.MappedTopicType.IotCore));
        expectedMapping.add(new TopicMapping.MappingEntry("mqtt/topic2", "/test/pubsub/topic",
                TopicMapping.MappedTopicType.Pubsub));
        expectedMapping.add(new TopicMapping.MappingEntry("mqtt/topic3", "/test/cloud/topic2",
                TopicMapping.MappedTopicType.IotCore));

        assertArrayEquals(expectedMapping.toArray(), mapping.getMapping().toArray());

        List<TopicMapping.MappingEntry> iotCoreMapping =
                mapping.getMappingsOfType(TopicMapping.MappedTopicType.IotCore);
        List<TopicMapping.MappingEntry> expectedIoTCoreMapping = new ArrayList<>();
        expectedIoTCoreMapping.add(new TopicMapping.MappingEntry("mqtt/topic", "/test/cloud/topic",
                TopicMapping.MappedTopicType.IotCore));
        expectedIoTCoreMapping.add(new TopicMapping.MappingEntry("mqtt/topic3", "/test/cloud/topic2",
                TopicMapping.MappedTopicType.IotCore));
        assertArrayEquals(expectedIoTCoreMapping.toArray(), iotCoreMapping.toArray());

        List<TopicMapping.MappingEntry> pubsubMapping = mapping.getMappingsOfType(TopicMapping.MappedTopicType.Pubsub);
        List<TopicMapping.MappingEntry> expectedPubsubMapping = new ArrayList<>();
        expectedPubsubMapping.add(new TopicMapping.MappingEntry("mqtt/topic2", "/test/pubsub/topic",
                TopicMapping.MappedTopicType.Pubsub));
        assertArrayEquals(expectedPubsubMapping.toArray(), pubsubMapping.toArray());
    }

    @Test
    void GIVEN_invalid_mapping_as_json_string_WHEN_updateMapping_THEN_mapping_not_updated() throws Exception {
        TopicMapping mapping = new TopicMapping();
        assertThat(mapping.getMapping().size(), is(equalTo(0)));
        Assertions.assertThrows(JsonProcessingException.class, () -> mapping.updateMapping("[\n"
                + "  {\"MqttTopic\": \"mqtt/topic\", \"MappedTopic\": \"/test/cloud/topic\", \"Type\": \"IotCore\"},\n"
                + "  {\"MqttTopic\": \"mqtt/topic2\", \"MappedTopic\": \"/test/pubsub/topic\", \"Type\": "
                + "\"Pubsub-Invalid\"},\n"
                + "  {\"MqttTopic\": \"mqtt/topic3\", \"MappedTopic\": \"/test/cloud/topic2\", \"Type\": \"IotCore\"}\n"
                + "]"));
        assertThat(mapping.getMapping().size(), is(equalTo(0)));
    }
}
