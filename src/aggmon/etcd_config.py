#!/usr/bin/python

import json
import logging
import os
import os.path
import sys
import time
import traceback
from etcd import *
from etcd_client import *
import pdb


log = logging.getLogger( __name__ )
DEFAULT_CONFIG_DIR = "../config.d"
ETCD_CONFIG_ROOT = "/config"

##
# default configuration data
##
DEFAULT_CONFIG = {
    "hierarchy": {
        "group": {
            # <flat_group_path> : {
            #                       "nodes" : [...],
            #                       "hpath" : <full hierarchy path string of this group> ## needed
            # }
        },
        "job": {
            # <jobid> : {
            #             "nodes" : [...],
            #             "hpath" : <full hierarchy path string> ## ?
            # }
        },
        "monnodes": {
            # <hostname> : {
            #                "hpath" : <full hierarchy path string> ## ?
            # }
        }
    },
    "services": {
        "collector": {
            "cwd": os.getcwd(),
            "cmd": "agg_collector",
            "cmd_opts": "--listen %(listen)s " +
            "--hierarchy-url %(hierarchy_url)s --state-file %(statefile)s --dispatcher %(dispatcher)s",
            "component_key": "group:host",
            "listen_port_range": "5262",
            "logfile": "/tmp/%(service)s_%(hierarchy)s_%(hierarchy_key)s.log",
            "per_group": True
        },
        "data_store": {
            "cwd": os.getcwd(),
            "cmd": "agg_datastore",
            "cmd_opts": "--listen %(listen)s " +
            "--dbname \"%(dbname)s\" --host \"%(dbhost)s\" " +
            "--hierarchy-url %(hierarchy_url)s --dispatcher %(dispatcher)s %(msgbus_opts)s",
            "cmdport_range": "5100-5199",
            "component_key":  "group:host",
            "listen_port_range": "5200-5299",
            "logfile": "/tmp/%(service)s_%(hierarchy)s_%(hierarchy_key)s.log",
            "per_group": True
        },
        "job_agg": {
            "cwd": os.getcwd(),
            "cmd": "agg_jobagg",
            "cmd_opts": "--listen %(listen)s --log debug " +
            "--jobid %(jobid)s --dispatcher %(dispatcher)s %(msgbus_opts)s",
            "component_key":  "jobid",
            "listen_port_range": "5300-5999",
            "logfile": "/tmp/%(service)s_%(jobid)s.log",
            "min_nodes": 4,
            "per_job": True
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
        "local_kill": "kill %(pid)d",
        "local_status": "ps --pid %(pid)d -o pid,wchan,cmd,args -ww | tail -1"
    },
    "agg-templates": {},
    "aggregate": {}
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
    def __init__(self, etcd_client, config_dir="."):
        self._config_dir = config_dir
        self.etcd_client = etcd_client

    def load_files(self, config=DEFAULT_CONFIG):
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
                hierarchy = cf.get("hierarchy", None)
                if isinstance(hierarchy, dict):
                    obj = hierarchy.get("group", None)
                    if isinstance(obj, dict):
                        config["hierarchy"]["group"].update(obj)
                    obj = hierarchy.get("job", None)
                    if isinstance(obj, dict):
                        config["hierarchy"]["job"].update(obj)
                    obj = hierarchy.get("monnodes", None)
                    if isinstance(obj, dict):
                        config["hierarchy"]["monnodes"].update(obj)
                services = cf.get("services", None)
                if isinstance(services, dict):
                    obj = services.get("collector", None)
                    if isinstance(obj, dict):
                        config["services"]["collector"].update(obj)
                    obj = services.get("data_store", None)
                    if isinstance(obj, dict):
                        config["services"]["data_store"].update(obj)
                    obj = services.get("job_agg", None)
                    if isinstance(obj, dict):
                        config["services"]["job_agg"].update(obj)
                database = cf.get("database", None)
                if isinstance(database, dict):
                    config["database"].update(database)
                resource_manager = cf.get("resource_manager", None)
                if isinstance(resource_manager, dict):
                    config["resource_manager"].update(resource_manager)
                glob = cf.get("global", None)
                if isinstance(glob, dict):
                    config["global"].update(glob)
    
            tpl = result.get("agg-templates", None)
            if isinstance(tpl, dict):
                templates.update(tpl)
    
            agg = result.get("agg", None)
            if isinstance(agg, dict):
                for k in sorted(agg.keys()):
                    a = agg[k]
                    if isinstance(a, dict):
                        aggregate.append(a)

        for agg in aggregate:
            tpl_names = agg.get("template", None)
            if tpl_names is None:
                continue
            del agg["template"]
            if not isinstance(tpl_names, dict):
                tpl_names = (tpl_names,)
            else:
                tpl_names = tpl_names.values()
            orig_attrs = agg.copy()
            agg.clear()
            for tpl_name in tpl_names:
                tpl = templates.get(tpl_name, None)
                if tpl is None:
                    raise Exception("Template '%s' used in config file '%s' is not known." %
                                    (tpl_name, f))
                agg.update(tpl)
            agg.update(orig_attrs)
            metrics = agg.get("metrics", None)
            if metrics is None:
                continue
            orig_metrics = agg["metrics"].copy()
            del agg["metrics"]
            agg["metrics"] = [orig_metrics[k] for k in sorted(orig_metrics.keys())]

        config["aggregate"] = aggregate
        return config

    def _get_nested_dicts_path(self, path):
        """
        Get value in the config nested dicts, represent the "path" to the value
        like a file system path. Each element is a key.
        This function is not used any more, it here only for reference.
        """
        if not path.startswith("/"):
            raise Exception("path '%s' does not start with /" % path)
        d = self._config
        for k in path.split("/")[1:]:
            if k in d:
                d = d[k]
                if d is None:
                    break
        log.debug("Config.get path=%s v=%r" % (path, d))
        return d

    def get(self, path):
        """
        Get the content of a path inside the config in etcd as a
        deserialized dict.
        """
        if not path.startswith("/"):
            raise EtcdInvalidKey
        if not path.startswith(ETCD_CONFIG_ROOT):
            path = ETCD_CONFIG_ROOT + path
        return self.etcd_client.deserialize(path)

    def init_etcd(self):
        """
        Initialize etcd config with values loaded from config files.
        The initialization process is potentially a race among multiple instances,
        therefore only the first one which does not find the config base directory
        will win and initialize the etcd tree.
        """
        try:
            self.etcd_client.write(ETCD_CONFIG_ROOT, None, dir=True, prevExist=False)
        except EtcdAlreadyExist:
            log.warning("etcd %s already exists." % ETCD_CONFIG_ROOT)
            return False
        config = self.load_files()
        try:
            self.etcd_client.update(ETCD_CONFIG_ROOT, config)
        except Exception as e:
            log.error("Failed to initialize config. %r" % e)
            self.etcd_client.delete(ETCD_CONFIG_ROOT, recursive=True, dir=True)
            return False
        return True

    def reinit_etcd(self):
        """
        Force re-initialization of the configuration while keeping the
        externally controlled hierarchy unmodified.
        This should be only triggered manually.

        """
        config = self.load_files()
        hierarchy = self.etcd_client.deserialize(ETCD_CONFIG_ROOT + "/hierarchy")
        config["hierarchy"] = hierarchy
        try:
            self.etcd_client.update(ETCD_CONFIG_ROOT, config)
        except Exception as e:
            log.error("Failed to initialize config. %r" % e)
            return False
        return True
        
