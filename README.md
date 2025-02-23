# CAN to MQTT Bridge

A bidirectional bridge between a CAN bus and an MQTT broker using Python.

## Features

- Receives CAN messages and publishes them to MQTT topics.
- Receives MQTT messages and sends them to the CAN bus.
- Uses a DBC file for decoding and encoding CAN messages.
- Handles unknown CAN messages.
- Configurable via a YAML file.

## Requirements

- Python 3.x
- See `requirements.txt` for Python dependencies.

## Installation

1. **Clone the repository:**

   ```bash
   git clone https://github.com/kranze/can2mqtt.git
   cd can2mqtt
