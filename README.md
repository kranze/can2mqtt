# can2mqtt
This repository contains a Python script that acts as a bidirectional bridge between a CAN bus and an MQTT broker. The script reads CAN messages, decodes them using a DBC file, and publishes them to MQTT topics. It also subscribes to MQTT topics, encodes the messages using the DBC file, and sends them onto the CAN bus.
