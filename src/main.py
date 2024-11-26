#!/usr/bin/env python3
"""
CAN to MQTT Bridge

This script acts as a bidirectional bridge between a CAN bus and an MQTT broker.
It reads CAN messages, decodes them using a DBC file, and publishes them to MQTT topics.
It also subscribes to MQTT topics, encodes the messages using the DBC file, and sends them on the CAN bus.

Features:
- Only publishes messages to MQTT when the content has changed.
- Handles unknown CAN messages by publishing their raw data.
- Retains MQTT messages and clears them upon program exit.
- Configurable via a YAML file.
"""

import binascii
import json
import time

import can
import cantools
import paho.mqtt.client as mqtt
import yaml

# Load configuration from YAML file
with open('config/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Retrieve configuration values
can_config = config['can_interface']
dbc_file = config['dbc_file']
mqtt_config = config['mqtt']

# Load the DBC file
db = cantools.database.load_file(dbc_file)

# Set up the CAN bus using configuration values
bus = can.interface.Bus(
    bustype=can_config['bustype'],
    channel=can_config['channel'],
    bitrate=can_config['bitrate'],
)

# Set up the MQTT client using configuration values

client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id=mqtt_config['client_id']
)
client.connect(
    mqtt_config['broker_address'],
    mqtt_config['broker_port'],
    60
)
client.enable_logger()  # Enable MQTT logging

# Dictionaries to store the last state of messages
last_known_messages = {}
last_unknown_messages = {}

# Set to keep track of published topics
published_topics = set()


def convert_named_signal_values(data):
    """
    Recursively convert NamedSignalValue instances in the data to their values.

    Args:
        data (dict, list, NamedSignalValue): The data structure containing signals.

    Returns:
        The data structure with NamedSignalValue instances converted to strings.
    """
    if isinstance(data, dict):
        return {k: convert_named_signal_values(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_named_signal_values(v) for v in data]
    elif isinstance(data, cantools.database.can.signal.NamedSignalValue):
        return str(data)  # Or use data.value if you prefer the numeric value
    else:
        return data


def on_can_message(msg):
    """
    Callback function for handling received CAN messages.

    Args:
        msg (can.Message): The received CAN message.

    Returns:
        None
    """
    try:
        # Retrieve the message definition from the DBC file
        message = db.get_message_by_frame_id(msg.arbitration_id)
        decoded = message.decode(msg.data)
        message_name = message.name

        # Convert NamedSignalValue instances to serializable types
        decoded_serializable = convert_named_signal_values(decoded)

        # Check if the content has changed
        if message_name in last_known_messages:
            last_decoded = last_known_messages[message_name]
            if decoded != last_decoded:
                # Publish the message
                topic = f"can/received/{message_name}"
                client.publish(
                    topic,
                    json.dumps(decoded_serializable),
                    retain=True
                )
                last_known_messages[message_name] = decoded
                published_topics.add(topic)
        else:
            # First time receiving this message
            topic = f"can/received/{message_name}"
            client.publish(
                topic,
                json.dumps(decoded_serializable),
                retain=True
            )
            last_known_messages[message_name] = decoded
            published_topics.add(topic)
    except KeyError:
        # Handle unknown message
        handle_unknown_message(msg)
    except Exception as e:
        print("Error processing CAN message:", e)


def handle_unknown_message(msg):
    """
    Handle unknown CAN messages by publishing their raw data over MQTT.

    Args:
        msg (can.Message): The unknown CAN message.

    Returns:
        None
    """
    try:
        message_id = msg.arbitration_id
        data_hex = binascii.hexlify(msg.data).decode('ascii')  # Convert data to hex string
        message_name = f"Unknown_{message_id}"

        # Create a dictionary to hold the data
        decoded = {
            "message_id": message_id,
            "data": data_hex
        }

        # Check if there is a previous state
        if message_id in last_unknown_messages:
            last_data = last_unknown_messages[message_id]
            if data_hex != last_data:
                # Data has changed, publish the message
                topic = f"can/received/{message_name}"
                client.publish(
                    topic,
                    json.dumps(decoded),
                    retain=True
                )
                last_unknown_messages[message_id] = data_hex
                published_topics.add(topic)
        else:
            # First time seeing this message ID, publish and store
            topic = f"can/received/{message_name}"
            client.publish(
                topic,
                json.dumps(decoded),
                retain=True
            )
            last_unknown_messages[message_id] = data_hex
            published_topics.add(topic)
    except Exception as e:
        print("Error handling unknown CAN message:", e)


# Set up the CAN message notifier
notifier = can.Notifier(bus, [on_can_message])


def on_mqtt_message(client, userdata, msg):
    """
    Callback function for handling received MQTT messages.

    Args:
        client (paho.mqtt.client.Client): The MQTT client instance.
        userdata: The private user data.
        msg (paho.mqtt.client.MQTTMessage): The received MQTT message.

    Returns:
        None
    """
    message_name = msg.topic.split('/')[-1]
    payload_str = msg.payload.decode()
    print(f"Received Topic: {msg.topic}")
    print(f"Received Payload: {payload_str}")
    try:
        # Retrieve the message definition from the DBC file
        message = db.get_message_by_name(message_name)
        data_dict = json.loads(payload_str)
        data = message.encode(data_dict)
        can_msg = can.Message(
            arbitration_id=message.frame_id,
            data=data,
            is_extended_id=False
        )
        bus.send(can_msg)
    except KeyError:
        # Handle unknown MQTT message
        handle_unknown_mqtt_message(message_name, payload_str)
    except json.JSONDecodeError as e:
        print("JSON parsing error:", e)
        print("Received Payload:", payload_str)
    except Exception as e:
        print("Error sending message:", e)


def handle_unknown_mqtt_message(message_name, payload_str):
    """
    Handle incoming MQTT messages for unknown CAN messages.

    Args:
        message_name (str): The name of the message extracted from the MQTT topic.
        payload_str (str): The payload of the MQTT message as a string.

    Returns:
        None
    """
    try:
        data_dict = json.loads(payload_str)
        message_id = data_dict.get("message_id")
        data_hex = data_dict.get("data")

        if message_id is None or data_hex is None:
            print("Invalid payload for unknown message.")
            return

        # Convert hex string back to bytes
        data_bytes = binascii.unhexlify(data_hex)
        can_msg = can.Message(
            arbitration_id=int(message_id),
            data=data_bytes,
            is_extended_id=False
        )
        bus.send(can_msg)
    except Exception as e:
        print("Error handling unknown MQTT message:", e)


# Set up MQTT callbacks and subscriptions
client.on_message = on_mqtt_message
client.subscribe("can/send/#")


def clear_retained_messages():
    """
    Clear retained MQTT messages upon program exit and perform cleanup.

    Returns:
        None
    """
    print("Clearing retained messages...")
    for topic in published_topics:
        # Publish empty message with retain=True to clear the retained message
        client.publish(topic, payload=None, retain=True)
    client.loop_stop()
    bus.shutdown()
    print("Cleanup complete.")


# Start the MQTT client loop
client.loop_start()

# Main loop to keep the script running
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    clear_retained_messages()
