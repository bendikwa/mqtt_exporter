# Prometheus exporter for MQTT

## Description:

Configurable general purpose prometheus exporter for MQTT
Subscribes to one or more MQTT topics, and lets you configure prometheus metrics based on pattern matching.

See exampleconf/metric_example.yaml for details.

## Python dependencies:

 - paho-mqtt
 - prometheus-client
 - PyYAML
 - yamlreader

## Usage:

- ./mqtt_exporter.py


## Config:

Yaml files in the folder config/ is combined and read as config.
See exampleconf/ for examples.


## Todo:

Add persistence of metrics on restart