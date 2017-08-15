#!/usr/bin/python2

import atexit
import argparse
import logging
import os
import platform
try:
    import pdb
except:
    pass
import re
import socket
import sys
import threading
import time
import traceback
try:
    import ujson as json
except ImportError:
    import json

from Queue import Queue, Empty
from agg_helpers import *
from agg_component import get_kwds, ComponentState, ComponentControl
from agg_config import Config, DEFAULT_CONFIG_DIR
from agg_rpc import *
from msg_tagger import MsgTagger


SERVICE_START_TIMEOUT = 120  # timeout for service start, in seconds
log = logging.getLogger( __name__ )
comp = None
etcd_client = None
running = True
kill_services = False


# Path Fix
sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.dirname(
                os.path.realpath(__file__)), "../")))


__doc__ = """
agg_control controls all aggmon related components, which are now separate processes
potentially running on separate hosts. It knows the entire administrative hierarchy
and orchestrates their interactions by calling them with appropriate arguments, such
that the components get linked with each other according to the hierarchy.

For each administrative (monitoring) group agg_control will start:
- one agg_collector instance. This instance waits for ZMQ PUSH messages with metric metadata or
  metric values. It handles subscriptions from other components like data_store or job aggregators.
- one data_store instance. data_store instances will subscribe to the metric messages
  published by their corresponding agg_pub_sub instances.

agg_control will receive tagger requests which it will pass on to all agg_control instances.
When job tagging requests are received, an add_tag will start an instance of the job aggregator
responsible for a particular job and tell it where to subscribe (all agg_control instances).
A remove_tag request for a jobid will kill the job aggregator instance.

Job aggregators will (for now) receive periodic requests for aggregation which agg_control will
push to them. The configuration, which metrics shall be aggregated and how, must be still designed.

agg_control writes a state file which contains the latest state of the started components.

In order to clean up the distributed system and kill all components, agg_control should be invoked
with the --kill command line option.
"""

def group_of_job(jobid, job_nodes, hosts_group):
    """
    Return the representative group hpath of a particular job.
    Compute the group deterministically, such that each controller gets the same result
    when computing it.

    Approach implemented here: determine in which group are most of the jobs nodes.
    """
    group_hpath = None
    group_histo = {}
    max_histo = -1
    for node in job_nodes:
        if node in hosts_group:
            gpath = hosts_group[node]
        else:
            log.warning("Host '%s' is in job %s but not in group hierarchy!" % (node, jobid))
            continue
        if gpath not in group_histo:
            group_histo[gpath] = 0
        group_histo[gpath] += 1
        if group_histo[gpath] > max_histo:
            max_histo = group_histo[gpath]
            group_hpath = gpath
    return group_hpath


def filter_group_jobs(jobs_config, hosts_group, own_groups_hpath):
    """
    Create list of jobids that should be handled by this controller.
    """
    own_jobids = []
    for jobid, job_config in jobs_config.items():
        if "nodes" not in job_config:
            log.warning("'nodes' not in job hierarchy info. What's wrong here?")
            continue
        if group_of_job(job_config["nodes"], hosts_group) in own_groups_hpath:
            own_jobids.append(jobid)
    return own_jobids


def calc_hosts_group(groups_config):
    """
    Return a dict with each host being a key with its group hpath as value.
    """
    hosts_group = {}
    for gk, gv in groups_config.items():
        if "nodes" in gv:
            gpath = gv["hpath"]
            for host in gv["nodes"]:
                hosts_group[host] = gpath
    return hosts_group


def aggmon_control(argv):
    global comp, etcd_client, running, kill_services

    ap = argparse.ArgumentParser()
    ap.add_argument('-c', '--config', default=DEFAULT_CONFIG_DIR,
                    action="store", help="configuration directory")
    ap.add_argument('-g', '--group', default=[], action="append",
                    help="group path that this collector should handle. " +
                    "Repeat for multiple groups. Example: -g /universe/r1")
    ap.add_argument('-l', '--log', default="info", action="store",
                    help="logging: info, debug, ...")
    ap.add_argument('-v', '--verbose', type=int, default=0, action="store",
                    help="verbosity")
    pargs = ap.parse_args(argv)

    log_level = eval("logging."+pargs.log.upper())
    FMT = "%(asctime)s %(levelname)-5.5s [%(name)s][%(threadName)s] %(message)s"
    logging.basicConfig( stream=sys.stderr, level=log_level, format=FMT )

    if len(pargs.group) == 0:
        log.error("No group path provided for this component. Use the -g option!")
        sys.exit(1)
    
    etcd_client = EtcdClient()
    if not etcd_client:
        log.error("Could not connect to etcd!")
        sys.exit(1)

    config = Config(etcd_client, config_dir=pargs.config)
    config.init_etcd()

    # groups for which this controller is responsible
    own_groups_hpath = pargs.group
    own_groups_keys = {}
    groups_config = config.get("/hierarchy/group")
    _groups_found = 0
    for group_key, gv in groups_config.items():
        if gv["hpath"] in own_groups_hpath:
            own_groups_keys[gv["hpath"]] = group_key
            _groups_found += 1
    if _groups_found < len(own_groups_hpath):
        log.error("Check your configuration! The groups this controller is responsible for could not be found.")
        os._exit(-1)

    state = {}
    running = True
    kill_services = False

    def reload_config(msg):
        return config.reinit_etcd()

    def quit(msg):
        global running
        comp.rpc.stop()
        running = False

    def kill_and_quit(msg):
        global running, kill_services
        comp.rpc.stop()
        kill_services = True
        running = False

    def start_in_progress(path):
        try:
            data = comp.get_data(path)
        except:
            return False
        if data.startswith("started "):
            data = data[8:]
            started = time.mktime(time.strptime(data))
            now = time.time()
            if now - started > 0 and now - started < SERVICE_START_TIMEOUT:
                return True
        return False

    state = get_kwds(own_groups=pargs.group)
    hostname = platform.node()
    
    comp = ComponentState(etcd_client, "control", "monnodes:/%s" % hostname, state=state)
    # start before registering RPCs means: consume old RPCs without action
    comp.start()
    #comp.rpc.register_rpc("add_group", add_group)
    #comp.rpc.register_rpc("remove_group", remove_group)
    #comp.rpc.register_rpc("show_groups", show_groups)
    comp.rpc.register_rpc("killquit", kill_and_quit, early_reply=True)
    comp.rpc.register_rpc("quit", quit, early_reply=True)
    comp.rpc.register_rpc("reload_config", reload_config)

    control = ComponentControl(config, etcd_client, comp)

    #########################################################
    # Main control logic
    #
    # ...
    # - check per_job subscribers and remove accordingly

    while running:
        #
        # get hierarchies info
        #
        jobs_config = config.get("/hierarchy/job")
        groups_config = config.get("/hierarchy/group")
        services_config = config.get("/services")

        hosts_group = calc_hosts_group(groups_config)
        own_jobids = filter_group_jobs(jobs_config, hosts_group, own_groups_hpath)

        # loop over services
        for svc_type in services_config.keys():
            svc_cfg = services_config[svc_type]
            #
            # per_group services
            #
            if "per_group" in svc_cfg and svc_cfg["per_group"] is True:
                #
                # is the service running in each of my groups?
                #
                for gpath in own_groups_hpath:
                    svc_state = comp.get_state(svc_type, "group:%s" % gpath)
                    if svc_state is None or svc_state == {}:
                        data_path = "/components/%s/group/%s" % (svc_type, own_groups_keys[gpath])
                        if start_in_progress(data_path):
                            continue
                        #
                        # start service!
                        #
                        ## TODO: select_host_to_run_on
                        control.start_component(svc_type, "group:%s" % gpath)
                        comp.set_data(data_path, "started %s" % time.ctime())
                #
                # TODO: handle deconfigured groups
                #

            #
            # per_job services
            #
            elif "per_job" in svc_cfg and svc_cfg["per_job"] is True:
                #
                #
                #
                for jobid in own_jobids:
                    svc_state = comp.get_state(svc_type, "job:/%s" % jobid)
                    if svc_state is None or svc_state == {}:
                        data_path = "/components/%s/job/%s" % (svc_type, jobid)
                        if start_in_progress(data_path):
                            continue
                        #
                        # start service!
                        #
                        control.start_component(svc_type, "job:/%s" % jobid)
                        comp.set_data(data_path, "started %s" % time.ctime())
                #
                # handle finished jobs
                #
                own_started_jobs = comp.get_data("/components/%s/job" % svc_type)
                if own_started_jobs is not None:
                    for jobid in set(own_started_jobs.keys()) - set(own_jobids):
                        svc_state = comp.get_state(svc_type, "job:/%s" % jobid)
                        if svc_state is not None:
                            #
                            # kill service!
                            #
                            control.kill_component(svc_type, "job:/%s" % jobid)
                        else:
                            comp.del_data("/components/%s/job/%s" % (svc_type, jobid))
        delay = 5 # seconds
        while running and delay > 0:
            time.sleep(1)
            delay -= 1

    if kill_services:
        own_services = comp.get_data("/components")
        for svc_type in own_services.keys():
            for hierarchy in own_services[svc_type].keys():
                for hierarchy_key in own_services[svc_type][hierarchy].keys():
                    hpath = config.get("/hierarchy/%s/%s/hpath" %
                                       (hierarchy, hierarchy_key))
                    svc_state = comp.get_state(svc_type, "%s:/%s" % (hierarchy, hpath))
                    if svc_state is not None:
                        control.kill_component(svc_type, "%s:/%s" % (hierarchy, hpath))
                        comp.del_data("/components/%s/%s/%s" %
                                      (svc_type, hierarchy, hierarchy_key))
    os._exit(0)


if __name__ == "__main__":
    aggmon_control(sys.argv[1:])

