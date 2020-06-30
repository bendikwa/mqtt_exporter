#!/usr/bin/env python

import prometheus_client as prometheus
from collections import defaultdict
import logging
import argparse
import paho.mqtt.client as mqtt
import yaml
import os
import re
import operator
import time
from yamlreader import yaml_load

VERSION = '1.1'


def _read_config(config_path):
    """Read config file from given location, and parse properties"""

    if config_path is not None:
        if os.path.isfile(config_path):
            logging.info(f'Config file found at: {config_path}')
            try:
                with open(config_path, 'r') as f:
                    return yaml.safe_load(f.read())
            except yaml.YAMLError:
                logging.exception('Failed to parse configuration file:')

        elif os.path.isdir(config_path):
            logging.info(
                f'Config directory found at: {config_path}')
            try:
                return yaml_load(config_path)
            except yaml.YAMLError:
                logging.exception('Failed to parse configuration directory:')

    return {}


def _parse_config_and_add_defaults(config_from_file):
    """Parse content of configfile and add default values where needed"""

    config = {}
    logging.debug(f'_parse_config Config from file: {str(config_from_file)}')
    # Logging values ('logging' is optional in config
    if 'logging' in config_from_file:
        config['logging'] = _add_config_and_defaults(
            config_from_file['logging'], {'logfile': '', 'level': 'info'})
    else:
        config['logging'] = _add_config_and_defaults(
            None, {'logfile': '', 'level': 'info'})

    # MQTT values
    if 'mqtt' in config_from_file:
        config['mqtt'] = _add_config_and_defaults(
            config_from_file['mqtt'], {'host': 'localhost'})
    else:
        config['mqtt'] = _add_config_and_defaults(None, {'host': 'localhost'})

    if 'auth' in config['mqtt']:
        config['mqtt']['auth'] = _add_config_and_defaults(
            config['mqtt']['auth'], {})
        _validate_required_fields(config['mqtt']['auth'], 'auth', ['username'])

    if 'tls' in config['mqtt']:
        config['mqtt']['tls'] = _add_config_and_defaults(
            config['mqtt']['tls'], {})

    # Prometheus values
    if 'prometheus' in config:
        config['prometheus'] = _add_config_and_defaults(
            config_from_file['prometheus'], {'exporter_port': 9344})
    else:
        config['prometheus'] = _add_config_and_defaults(
            None, {'exporter_port': 9344})

    metrics = {}
    for metric in config_from_file['metrics']:
        parse_and_validate_metric_config(metric, metrics)

    config['metrics'] = _group_by_topic(list(metrics.values()))
    return config


def parse_and_validate_metric_config(metric, metrics):
    m = _add_config_and_defaults(metric, {})
    _validate_required_fields(m, None, ['name', 'help', 'type', 'topic'])
    if 'label_configs' in m and m['label_configs']:
        label_configs = []
        for lc in m['label_configs']:
            if lc:
                lc = _add_config_and_defaults(lc, {'separator': ';', 'regex': '^(.*)$', 'replacement': '\\1',
                                                   'action': 'replace'})
                if lc['action'] == 'replace':
                    _validate_required_fields(lc, None,
                                              ['source_labels', 'target_label', 'separator', 'regex', 'replacement',
                                               'action'])
                else:
                    _validate_required_fields(lc, None,
                                              ['source_labels', 'separator', 'regex', 'replacement',
                                               'action'])
                label_configs.append(lc)
        m['label_configs'] = label_configs
    metrics[m['name']] = m


def _validate_required_fields(config, parent, required_fields):
    """Fail if required_fields is not present in config"""
    for field in required_fields:
        if field not in config or config[field] is None:
            if parent is None:
                error = f'\'{field}\' is a required field in configfile'
            else:
                error = f'\'{field}\' is a required parameter for field {parent} in configfile'
            raise TypeError(error)


def _add_config_and_defaults(config, defaults):
    """Return dict with values from config, if present, or values from defaults"""
    if config is not None:
        defaults.update(config)
    return defaults.copy()


def _strip_config(config, allowed_keys):
    return {k: v for k, v in config.items() if k in allowed_keys and v}


def _group_by_topic(metrics):
    """Group metrics by topic"""
    t = defaultdict(list)
    for metric in metrics:
        t[metric['topic']].append(metric)
    return t


def _topic_matches(topic1, topic2):
    """Check if wildcard-topics match"""
    if topic1 == topic2:
        return True

    # If topic1 != topic2 and no wildcard is present in topic1, no need for regex
    if '+' not in topic1 and '#' not in topic1:
        return False

    logging.debug(
        f'_topic_matches: Topic1: {topic1}, Topic2: {topic2}')
    topic1 = re.escape(topic1)
    regex = topic1.replace('/\\#', '.*$').replace('\\+', '[^/]+')
    match = re.match(regex, topic2)

    logging.debug(f'_topic_matches: Match: {match is not None}')
    return match is not None


# noinspection SpellCheckingInspection
def _log_setup(logging_config):
    """Setup application logging"""

    logfile = logging_config['logfile']

    log_level = logging_config['level']

    numeric_level = logging.getLevelName(log_level.upper())
    if not isinstance(numeric_level, int):
        raise TypeError(f'Invalid log level: {log_level}')

    if logfile != '':
        logging.info('Logging redirected to: ' + logfile)
        # Need to replace the current handler on the root logger:
        file_handler = logging.FileHandler(logfile, 'a')
        formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
        file_handler.setFormatter(formatter)

        log = logging.getLogger()  # root logger
        for handler in log.handlers:  # remove all old handlers
            log.removeHandler(handler)
        log.addHandler(file_handler)

    else:
        logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s')

    logging.getLogger().setLevel(numeric_level)
    logging.info(f'log_level set to: {log_level}')


# noinspection PyUnusedLocal
def _on_connect(client, userdata, flags, rc):
    """The callback for when the client receives a CONNACK response from the server."""
    logging.info(f'Connected to broker, result code {str(rc)}')

    for topic in userdata.keys():
        client.subscribe(topic)
        logging.info(f'Subscribing to topic: {topic}')


def _label_config_match(label_config, labels):
    """Action 'keep' and 'drop' in label_config: Matches joined 'source_labels' to 'regex'"""
    source = label_config['separator'].join(
        [labels[x] for x in label_config['source_labels']])
    logging.debug(f'_label_config_match source: {source}')
    match = re.match(label_config['regex'], source)

    if label_config['action'] == 'keep':
        logging.debug(
            f"_label_config_match Action: {label_config['action']}, Keep msg: {match is not None}")
        return match is not None
    if label_config['action'] == 'drop':
        logging.debug(
            f"_label_config_match Action: {label_config['action']}, Drop msg: {match is not None}")
        return match is None
    else:
        logging.debug(
            f"_label_config_match Action: {label_config['action']} is not supported, metric is dropped")
        return False


def _apply_label_config(labels, label_configs):
    """Create/change labels based on label_config in config file."""

    for label_config in label_configs:
        if label_config['action'] == 'replace':
            _label_config_rename(label_config, labels)
        else:
            if not _label_config_match(label_config, labels):
                return False
    return True


def _label_config_rename(label_config, labels):
    """Action 'rename' in label_config: Add/change value for label 'target_label'"""
    source = label_config['separator'].join(
        [labels[x] for x in label_config['source_labels']])

    if re.match(re.compile(label_config['regex']), source):
        logging.debug(f'_label_config_rename source: {source}')
        result = re.sub(label_config['regex'],
                        label_config['replacement'], source)
        logging.debug(f'_label_config_rename result: {result}')
        labels[label_config['target_label']] = result


def finalize_labels(labels):
    """Keep '__value__', and '__topic__' but remove all other labels starting with '__'"""
    labels['value'] = labels['__value__']
    labels['topic'] = labels['__topic__']

    return {k: v for k, v in labels.items() if not k.startswith('__')}


def _update_metrics(metrics, msg):
    """For each metric on this topic, apply label renaming if present, and export to prometheus"""
    for metric in metrics:
        labels = {'__topic__': metric['topic'],
                  '__msg_topic__': msg.topic, '__value__': str(msg.payload, 'utf-8')}

        if 'label_configs' in metric:
            # If action 'keep' in label_configs fails, or 'drop' succeeds, the metric is not updated
            if not _apply_label_config(labels, metric['label_configs']):
                continue

        try:
            labels['__value__'] = float(labels['__value__'].replace(',', '.'))
        except ValueError:
            logging.exception(
                f"__value__ must be a number, was: {labels['__value__']}, Metric: {metric['name']} on __msg_topic_: {labels['__msg_topic__']}")
            continue

        logging.debug('_update_metrics all labels:')
        logging.debug(labels)

        labels = finalize_labels(labels)

        _export_to_prometheus(metric['name'], metric, labels)


# noinspection PyUnusedLocal
def _on_message(client, userdata, msg):
    """The callback for when a PUBLISH message is received from the server."""
    logging.debug(
        f'_on_message Msg received on topic: {msg.topic}, Value: {str(msg.payload)}')

    for topic in userdata.keys():
        if _topic_matches(topic, msg.topic):
            _update_metrics(userdata[topic], msg)


def _mqtt_init(mqtt_config, metrics):
    """Setup mqtt connection"""
    mqtt_client = mqtt.Client(userdata=metrics)
    mqtt_client.on_connect = _on_connect
    mqtt_client.on_message = _on_message

    if 'auth' in mqtt_config:
        auth = _strip_config(mqtt_config['auth'], ['username', 'password'])
        mqtt_client.username_pw_set(**auth)

    if 'tls' in mqtt_config:
        tls_config = _strip_config(mqtt_config['tls'], [
                                   'ca_certs', 'certfile', 'keyfile', 'cert_reqs', 'tls_version'])
        mqtt_client.tls_set(**tls_config)

    mqtt_client.connect(**_strip_config(mqtt_config,
                                        ['host', 'port', 'keepalive']))
    return mqtt_client


def _export_to_prometheus(name, metric, labels):
    """Export metric and labels to prometheus."""
    valid_types = ['gauge', 'counter', 'summary', 'histogram']
    if metric['type'] not in valid_types:
        logging.warning(
            f"Metric type: {metric['type']}, is not a valid metric type. Must be one of: {valid_types}")

    value = labels['value']
    del labels['value']

    sorted_labels = _get_sorted_tuple_list(labels)
    label_names, label_values = list(zip(*sorted_labels))

    prometheus_metric_types = {'gauge': gauge,
                               'counter': counter,
                               'summary': summary,
                               'histogram': histogram}

    try:
        prometheus_metric_types[metric['type'].lower()](
            label_names, label_values, metric, name, value)
        logging.debug(
            f'_export_to_prometheus metric {name}{labels} updated with value: {value}')
    except KeyError:
        logging.warning(
            f"Metric type: {metric['type']}, is not a valid metric type. Must be one of: {list(prometheus_metric_types.keys())}")


def gauge(label_names, label_values, metric, name, value):
    """Define metric as Gauge, setting it to 'value'"""
    get_prometheus_metric(label_names, label_values, metric, name).set(value)


def get_prometheus_metric(label_names, label_values, metric, name, buckets=None):
    key = ':'.join([''.join(label_names), ''.join(label_values)])
    if 'prometheus_metric' not in metric or not metric['prometheus_metric']:
        metric['prometheus_metric'] = {}
        prometheus_metric_types = {'gauge': prometheus.Gauge,
                                   'counter': prometheus.Counter,
                                   'summary': prometheus.Summary,
                                   'histogram': prometheus.Histogram}

        metric_type = metric['type'].lower()
        if metric_type == 'histogram' and buckets:
            metric['prometheus_metric']['base'] = prometheus_metric_types[metric_type](name, metric['help'],
                                                                                       list(label_names), buckets)
        else:
            metric['prometheus_metric']['base'] = prometheus_metric_types[metric_type](name, metric['help'],
                                                                                       list(label_names))

    if key not in metric['prometheus_metric'] or not metric['prometheus_metric'][key]:
        metric['prometheus_metric'][key] = metric['prometheus_metric']['base'].labels(
            *list(label_values))
    return metric['prometheus_metric'][key]


def counter(label_names, label_values, metric, name, value):
    """Define metric as Counter, increasing it by 'value'"""
    get_prometheus_metric(label_names, label_values, metric, name).inc(value)


def summary(label_names, label_values, metric, name, value):
    """Define metric as summary, observing 'value'"""
    get_prometheus_metric(label_names, label_values,
                          metric, name).observe(value)


def histogram(label_names, label_values, metric, name, value):
    """Define metric as histogram, observing 'value'"""
    buckets = None
    if 'buckets' in metric and metric['buckets']:
        buckets = metric['buckets'].split(',')

    get_prometheus_metric(label_names, label_values,
                          metric, name, buckets).observe(value)


def add_static_metric(timestamp):
    g = prometheus.Gauge('mqtt_exporter_timestamp', 'Startup time of exporter in millis since EPOC (static)',
                         ['exporter_version'])
    g.labels(VERSION).set(timestamp)


def _get_sorted_tuple_list(source):
    """Return a sorted list of tuples"""
    filtered_source = source.copy()
    sorted_tuple_list = sorted(
        list(filtered_source.items()), key=operator.itemgetter(0))
    return sorted_tuple_list


def main():
    add_static_metric(int(time.time() * 1000))
    # Setup argument parsing
    parser = argparse.ArgumentParser(
        description='Simple program to export formatted mqtt messages to prometheus')
    parser.add_argument('-c', '--config', action='store', dest='config', default='conf',
                        help='Set config location (file or directory), default: \'conf\'')
    options = parser.parse_args()

    # Initial logging to console
    _log_setup({'logfile': '', 'level': 'info'})

    # Read config file from disk
    from_file = _read_config(options.config)
    config = _parse_config_and_add_defaults(from_file)

    # Set up logging
    _log_setup(config['logging'])

    # Start prometheus exporter
    logging.info(
        f"Starting prometheus exporter on port: {str(config['prometheus']['exporter_port'])}")
    prometheus.start_http_server(config['prometheus']['exporter_port'])

    # Set up mqtt client and loop forever
    mqtt_client = _mqtt_init(config['mqtt'], config['metrics'])
    mqtt_client.loop_forever()


if __name__ == '__main__':
    main()
