# Prometheus exporter for MQTT

## Description:

Configurable general purpose prometheus exporter for MQTT.

Subscribes to one or more MQTT topics, and lets you configure prometheus metrics based on pattern matching.

See exampleconf/metric_example.yaml for details.

## Usage:

- Create a folder to hold the config (default: "config/")
- Add metric config(s) in yaml format to the folder. (See exampleconf/metric_example.yaml for details)
- Run  ./mqtt_exporter.py
- Profit!

## Config:

Yaml files in the folder config/ is combined and read as config.
See exampleconf/ for examples.

## Python dependencies:

 - paho-mqtt
 - prometheus-client
 - PyYAML
 - yamlreader

## Todo:

Add persistence of metrics on restart