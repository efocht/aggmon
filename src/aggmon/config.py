#!/usr/bin/python

import etcd
import json
import logging
import os
import os.path
import sys
import traceback


log = logging.getLogger( __name__ )


##
# default configuration data
##
DEFAULT_CONFIG = {
    "groups": {
    #     "/universe": {
    #         "job_agg_nodes": ["localhost"],
    #         "data_store_nodes" : ["localhost"],
    #         "collector_nodes" : ["localhost"]
    #     }
    },
    "hierarchy": {
        "monitor": {
            "group": [
                ]
            },
        "job": {
            "jobid": [
                ]
            }
    },
    "services": {
        "collector": {
            "cwd": os.getcwd(),
            "cmd": "agg_collector",
            "cmd_opts": "--cmd-port %(cmdport)s --listen %(listen)s " +
            "--group %(group_path)s --state-file %(statefile)s --dispatcher %(dispatcher)s",
            "cmdport_range": "5100-5199",
            "component_key": ["group", "host"],
            "listen_port_range": "5262",
            "logfile": "/tmp/%(service)s_%(group)s.log"
        },
        "data_store": {
            "cwd": os.getcwd(),
            "cmd": "agg_datastore",
            "cmd_opts": "--cmd-port %(cmdport)s --listen %(listen)s " +
            "--dbname \"%(dbname)s\" --host \"%(dbhost)s\" " +
            "--group %(group_path)s --dispatcher %(dispatcher)s %(msgbus_opts)s",
            "cmdport_range": "5100-5199",
            "component_key":  ["group", "host"],
            "listen_port_range": "5200-5299",
            "logfile": "/tmp/%(service)s_%(group)s.log"
        },
        "job_agg": {
            "cwd": os.getcwd(),
            "cmd": "agg_jobagg",
            "cmd_opts": "--cmd-port %(cmdport)s --listen %(listen)s --log debug " +
            "--jobid %(jobid)s --dispatcher %(dispatcher)s %(msgbus_opts)s",
            "cmdport_range": "5000-5999",
            "component_key":  ["jobid"],
            "listen_port_range": "5300-5999",
            "logfile": "/tmp/%(service)s_%(jobid)s.log",
            "min_nodes": 4
        }
    },
    "database": {
        "dbname": "metricdb",
        "jobdbname": "metric",
        "dbhost": "localhost:27017",
        "user": "",
        "password": ""
    },
    "resource_manager": {
        "type": "pbs",
        "master": "",
        "ssh_port": 22,
        "pull_state_cmd": ""
    },
    "global": {
        "local_cmd"  : "cd %(cwd)s; %(cmd)s >%(logfile)s 2>&1 &",
        "remote_cmd" : "ssh %(host)s \"cd %(cwd)s; %(cmd)s >%(logfile)s 2>&1 &\"",
        "remote_kill": "ssh %(host)s kill %(pid)d",
        "remote_status": "ssh %(host)s \"ps --pid %(pid)d -o pid,wchan,cmd,args -ww | tail -1\""
    },
    "agg-templates": [],
    "aggregate": []
}

# default aggregation config
# cmd : "agg"
# metric : metric that should be aggregated
# agg_metric : aggregated metric name
# push_target : where to push the aggregated metric to. Can be the agg_collector
#               of the own group or one on a higher level or the data store.
# agg_type : aggregation type, i.e. min, max, avg, sum, worst, quant10
# ttl : (optional) time to live for metrics, should filter out old/expired metrics
# args ... : space for further aggregator specific arguments


class Config(object):
    def __init__(self, config=DEFAULT_CONFIG, config_dir="."):
        self._config = config
        self._config_dir = config_dir
        self.load_files()


    def load_files(self):
        try:
            import yaml
        except ImportError as e:
            print("Cannot find python module yaml")
            raise e
    
        files = []
        if os.path.isdir(self._config_dir):
            for f in os.listdir(self._config_dir):
                path = os.path.join(self._config_dir, f)
                if os.path.isfile(path) and path.endswith(".yaml"):
                    files.append(path)
        else:
            raise Exception("config_dir '%s' is not a directory!" % self._config_dir)
    
        templates = {}
        aggregate = []
    
        for f in files:
            result = yaml.safe_load(open(f))
            cf = result.get("config", None)
            if cf is not None:
                groups = cf.get("groups", None)
                if isinstance(groups, dict):
                    self._config["groups"].update(groups)
                services = cf.get("services", None)
                if isinstance(services, dict):
                    obj = services.get("collector", None)
                    if isinstance(obj, dict):
                        self._config["services"]["collector"].update(obj)
                    obj = services.get("data_store", None)
                    if isinstance(obj, dict):
                        self._config["services"]["data_store"].update(obj)
                    obj = services.get("job_agg", None)
                    if isinstance(obj, dict):
                        self._config["services"]["job_agg"].update(obj)
                database = cf.get("database", None)
                if isinstance(database, dict):
                    self._config["database"].update(database)
                resource_manager = cf.get("resource_manager", None)
                if isinstance(resource_manager, dict):
                    self._config["resource_manager"].update(resource_manager)
                glob = cf.get("global", None)
                if isinstance(glob, dict):
                    self._config["global"].update(glob)
    
            tpl = result.get("agg-templates", None)
            if isinstance(tpl, dict):
                templates.update(tpl)
    
            agg = result.get("agg", None)
            if isinstance(agg, list):
                for a in agg:
                    if isinstance(a, dict):
                        aggregate.append(a)
    
        for agg in aggregate:
            tpl_names = agg.get("template", None)
            if tpl_names is None:
                continue
            del agg["template"]
            if not isinstance(tpl_names, list):
                tpl_names = (tpl_names,)
            orig_attrs = agg.copy()
            agg.clear()
            for tpl_name in tpl_names:
                tpl = templates.get(tpl_name, None)
                if tpl is None:
                    raise Exception("Template '%s' used in config file '%s' is not known." %
                                    (tpl_name, f))
                agg.update(tpl)
            agg.update(orig_attrs)
    
        self._conf["aggregate"] = aggregate

    def geta(self, *path):
        d = self._config
        for k in path:
            if k in d:
                d = d[k]
                if d is None:
                    break
        return d

    def get(self, path):
        """Get value in the config nested dicts, represent the "path" to the value
        like a file system path. Each element is a key.
        """
        if not path.startswith("/"):
            raise Exception("path '%s' does not start with /" % path)
        d = self._config
        for k in path.split("/")[1:]:
            if k in d:
                d = d[k]
                if d is None:
                    break
        return d

