## AWS Greengrass MQTT Bridge

The MQTT bridge component (aws.greengrass.clientdevices.mqtt.Bridge) relays MQTT messages between client devices, local Greengrass publish/subscribe, and AWS IoT Core. You can use this component to act on MQTT messages from client devices in custom components and sync client devices with the AWS Cloud.

You can use this component to relay messages between the following message brokers:
* Local MQTT – The local MQTT broker handles messages between client devices and a core device.
* Local publish/subscribe – The local Greengrass message broker handles messages between components on a core device. For more information about how to interact with these messages in Greengrass components, see [Publish/subscribe local messages](https://docs.aws.amazon.com/greengrass/v2/developerguide/ipc-publish-subscribe.html).
* AWS IoT Core – The AWS IoT Core MQTT broker handles messages between IoT devices and AWS Cloud destinations. For more information about how to interact with these messages in Greengrass components, see [Publish/subscribe AWS IoT Core MQTT messages](https://docs.aws.amazon.com/greengrass/v2/developerguide/ipc-iot-core-mqtt.html).

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
