#! /usr/bin/env python

import collectd
import json
import urllib2
import os
import time
import datetime
import requests
from docker import Client

CONFIGS = []

METRICS = {
    "cpus_limit": 1000,
    "cpus_system_time_secs": 1000,
    "cpus_user_time_secs": 1000,
    "mem_limit_bytes": 1,
    "mem_rss_bytes": 1,
    "docker_cpu_total": 1,
    "docker_cpu_system": 1,
    "docker_memory_limit": 1,
    "docker_memory_usage": 1,
    "docker_cpu_percent": 1,
    "docker_cpu_throttling_time": 1
}

prev_docker_stats = {}

def container_name(container):
    return container['Names'][0]

def get_stats(container, cli):
    stats = cli.stats(container, decode = True)
    for stat in stats:
        return stat

def get_docker_stats():
    """Fetch docker stats"""

    global prev_docker_stats
    cli = Client(base_url='tcp://0.0.0.0:2375')
    containers = map(container_name, cli.containers())
    result = {}
    stats = {}
    stats_after_delay = {}
    for container in containers:
        stats[container] = get_stats(container, cli)

    for container in containers:
        inspection = cli.inspect_container(container)
        envs =  inspection['Config']['Env']
        task_id = next((name for name in envs if name.split('=')[0] == 'MESOS_TASK_ID' or name.split('=')[0] == 'mesos_task_id'), "")
        if task_id =="":
            if '.' in container:
                task_id = container.split('.')[1]
        else:
            task_id = task_id.split('=')[1]

        if task_id !="":
            if task_id in prev_docker_stats:
                cpu_percent = (stats[container]['cpu_stats']['cpu_usage']['total_usage']- prev_docker_stats[task_id]['docker_cpu_total']) / float(stats[container]['cpu_stats']['system_cpu_usage'] - prev_docker_stats[task_id]['docker_cpu_system']) * 1000 * len(stats[container]['cpu_stats']['cpu_usage']['percpu_usage'])
            else:
                cpu_percent = 0
            result[task_id] = {
                'docker_memory_usage': stats[container]['memory_stats']['usage'],
                'docker_memory_limit': stats[container]['memory_stats']['limit'],
                'docker_cpu_total': stats[container]['cpu_stats']['cpu_usage']['total_usage'],
                'docker_cpu_percent': cpu_percent,
                'docker_cpu_system': stats[container]['cpu_stats']['system_cpu_usage'],
                'docker_cpu_throttling_time': stats[container]['cpu_stats']['throttling_data']['throttled_time']
            }

    prev_docker_stats = result
    return result

def configure_callback(conf):
    """Receive configuration"""

    host = "127.0.0.1"
    port = 5051
    post_endpoint = ""

    for node in conf.children:
        if node.key == "PostEndpoint":
            post_endpoint = node.values[0]
        elif node.key == "Host":
            host = node.values[0]
        elif node.key == "Port":
            port = int(node.values[0])
        else:
            collectd.warning("mesos-tasks plugin: Unknown config key: %s." % node.key)

    CONFIGS.append({
        "host": host,
        "port": port,
        "post_endpoint": post_endpoint
    })

def fetch_json(url):
    """Fetch json from url"""
    try:
        return json.load(urllib2.urlopen(url, timeout=5))
    except urllib2.URLError, e:
        collectd.error("mesos-tasks plugin: Error connecting to %s - %r" % (url, e))
        return None

def fetch_metrics(conf):
    """Fetch metrics from slave"""
    return fetch_json("http://%s:%d/monitor/statistics.json" % (conf["host"], conf["port"]))

def fetch_state(conf):
    """Fetch state from slave"""
    return fetch_json("http://%s:%d/state.json" % (conf["host"], conf["port"]))

def node_group():
    group = 'default'
    if os.path.isfile('/etc/statsd-node-group'):
        with open('/etc/statsd-node-group', 'r') as f:
            group =  f.readline()
    return group

def read_stats(conf):
    """Read stats from specified slave"""
    metrics = fetch_metrics(conf)
    state = fetch_state(conf)

    if metrics is None or state is None:
        return

    tasks = {}
    docker_stats = get_docker_stats()

    for framework in state["frameworks"]:
        for executor in framework["executors"]:
            for task in executor["tasks"]:
                info = {}

                labels = {}
                if "labels" in task:
                    for label in task["labels"]:
                        labels[label["key"]] = label["value"]

                info["labels"] = labels
                info["framework_name"] = framework["name"]
                info["task_name"] = task["name"]
                info["container"] = executor["container"]

                tasks[framework["id"] + "-"+  task["id"]] = info

    for task in metrics:
        key = task["framework_id"] + "-" + task["source"]
        if key not in tasks:
            collectd.warning("mesos-tasks plugin: Task %s found in metrics, but missing in state" % task["source"])
            continue

        info = tasks[key]
        if "collectd_app" in info["labels"]:
            app = info["labels"]["collectd_app"].replace(".", "_") + '.' + task["source"].replace(".", "_")
        else:
            app = info['framework_name'].replace(".", "_")  + '.' + info['task_name'].replace(".", "_")

        stats = task["statistics"]

        if task["source"] in docker_stats:
            stats=dict(stats.items() + docker_stats[task["source"]].items())
        elif info["container"] in docker_stats:
            stats=dict(stats.items() + docker_stats[info["container"]].items())

        for metric, multiplier in METRICS.iteritems():
            if metric not in stats:
                continue

            val = collectd.Values(plugin="mesos-tasks")
            val.type = "gauge"
            val.plugin_instance = app
            val.type_instance = metric
            val.values = [int(stats[metric] * multiplier)]
            val.dispatch()
        send_post(info, stats, conf)

def send_post(info, stats, conf):
    if conf["post_endpoint"] == "":
        return

    payload = { 'framework_name': info["framework_name"], 'task_name': info["task_name"], 'host': conf["host"], 'timestamp': datetime.datetime.utcnow().isoformat() }

    for metric, multiplier in METRICS.iteritems():
        if metric not in stats:
            continue
        payload[metric] = int(stats[metric] * multiplier)
    try:
        requests.post(conf["post_endpoint"], data=json.dumps(payload))
    except:
        collectd.error("error during sending %s, %s" % (json.dumps(payload), conf["post_endpoint"]))

def read_callback():
    """Read stats from configured slaves"""
    for conf in CONFIGS:
        read_stats(conf)

collectd.register_config(configure_callback)
collectd.register_read(read_callback)
