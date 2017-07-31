#!/usr/bin/python2

import atexit
import argparse
import logging
import os
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
from etcd_component import get_kwds, ComponentState
from etcd_config import Config, DEFAULT_CONFIG_DIR
from etcd_rpc import *
from msg_tagger import MsgTagger


log = logging.getLogger( __name__ )
comp = None
etcd_client = None

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

def aggmon_control(argv):
    global comp, etcd_client

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
    
    config = Config(config_dir=pargs.config)

    # groups for which this controller is responsible
    own_groups_hpath = pargs.group
    own_groups_keys = []
    groups_config = config.get("/hierarchy/group")
    for group_key, gv in groups_config.items():
        if gv["hpath"] in own_groups_hpath:
            own_groups_keys.append(group_key)

    state = {}

    def quit(msg):
        comp.rpc.stop()
        running = False
        #subq.stopping = True
        # raw exit for now
        os._exit(0)

    #atexit.register(join_threads, subq)

    etcd_client = EtcdClient()
    state = get_kwds(own_groups=groups)

    hostname = etcd_client.members["name"]   # could also use "id"
    
    comp = ComponentState(etcd_client, "control", "monnodes:/%s" % hostname, state=state)
    comp.start()
    #comp.rpc.register_rpc("add_group", add_group)
    #comp.rpc.register_rpc("remove_group", remove_group)
    #comp.rpc.register_rpc("show_groups", show_groups)
    comp.rpc.register_rpc("quit", quit, early_reply=True)

    #########################################################
    # Main control logic
    #
    # ...
    # - check per_job subscribers and remove accordingly

    running = True
    while (running):
        #
        # get hierarchies info
        #
        jobs_config = config.get("/hierarchy/job")
        groups_config = config.get("/hierarchy/group")
        services_config = config.get("/services")

        own_jobids = filter_group_jobs(jobs_config, own_groups_hpath)
        
        # loop over services
        for svc_type in services_config.keys():
            svc_cfg = services_config[service_type]
            #
            # per_group services
            #
            if "per_group" in svc_cfg and svc_cfg["per_group"] is True:
                #
                # is the service running in each of my groups?
                #
                for gpath in own_groups_hpath:
                    svc_state = comp.get_state(svc_type, "group:%s" % gpath)
                    if svc_state is None:
                        #
                        # start service!
                        #
                        ## select_host_to_run_on
                        start_service(svc_type, "group:%s" % gpath)
            #
            # per_job services
            #
            elif "per_job" in svc_cfg and svc_cfg["per_job"] is True:
                #
                #
                #
                for jobid in own_jobids:
                    svc_state = comp.get_state(svc_type, "job:/%s" % jobid)
                    if svc_state is None:
                        #
                        # start service!
                        #
                        start_service(svc_type, "job:/%s" % jobid)
            #
            # TODO: handle finished jobs
            # TODO: handle deconfigured groups
            #


def join_threads(subq):
    try:
        subq.join(0.1)
    except Exception as e:
        log.error("main thread exception: %r" % e)
    log.debug("leaving...")


if __name__ == "__main__":
    aggmon_control(sys.argv)

