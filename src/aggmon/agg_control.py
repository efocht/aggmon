#!/usr/bin/python2

import argparse
import copy
import json
import logging
import os
import pdb
import sys
import time
import zmq
from agg_component import ComponentStatesRepo, group_name
from agg_rpc import send_rpc, zmq_own_addr_for_uri, RPCThread
from scheduler import Scheduler
from repeat_event import RepeatEvent
# Path Fix
sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.dirname(
                os.path.realpath(__file__)), "../")))
from metric_store.mongodb_store import *


log = logging.getLogger( __name__ )
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

When agg_control (re)starts and finds a state file, it will wait for ~70s and listen for component
state messages from previously spawned components. The configured state will be restored after
this time period, i.e. components will be killed and spawned as configured (if the state differs)
and they will be requested to re-subscribe.

In order to clean up the distributed system and kill all components, agg_control should be invoked
with the --kill command line option.
"""
# hierarchy: component -> group/host
component_states = None

# job_agg timers indexed by jobid
jagg_timers = {}

# own (dispatcher) rpc port
me_rpc = ""

# periodic events scheduler
scheduler = None

# ZeroMQ context (global)
zmq_context = None

# list of running jobs, as fresh as the mongodb collection
job_list = []

##
# default configuration data
##
config = {
    "groups": {
    #     "/universe": {
    #         "job_agg_nodes": ["localhost"],
    #         "data_store_nodes" : ["localhost"],
    #         "collector_nodes" : ["localhost"]
    #     }
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
    "global": {
        "local_cmd"  : "cd %(cwd)s; %(cmd)s >%(logfile)s 2>&1 &",
        "remote_cmd" : "ssh %(host)s \"cd %(cwd)s; %(cmd)s >%(logfile)s 2>&1 &\"",
        "remote_kill": "ssh %(host)s kill %(pid)d"
    }
}

# default aggregation config
# cmd : "agg"
# metric : metric that should be aggregated
# agg_metric : aggregated metric name
# push_target : where to push the aggregated metric to. Can be the agg_collector
#               of the own group or one on a higher level or the mongo store.
# agg_type : aggregation type, i.e. min, max, avg, sum, worst, quant10
# ttl : (optional) time to live for metrics, should filter out old/expired metrics
# args ... : space for further aggregator specific arguments
aggregate = {}


def load_config( config_file ):

    global config, aggregate

    import yaml

    files = []
    if os.path.isdir(config_file):
        for f in os.listdir(config_file):
            path = os.path.join(config_file, f)
            if os.path.isfile(path):
                files.append(path)
    else:
        files = (config_file,)

    templates = {}
    aggregate = []

    for f in files:
        result = yaml.safe_load(open(f))
        cf = result.get("config", None)
        if cf is not None:
            groups = cf.get("groups", None)
            if isinstance(groups, dict):
                config["groups"].update(groups)
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
            glob = cf.get("global", None)
            if isinstance(glob, dict):
                config["global"].update(glob)

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
                raise Exception("Template '%s' used in config file '%s' is not known." % (tpl_name, f))
            agg.update(tpl)
        agg.update(orig_attrs)


def get_collectors_cmd_ports():
    global component_states
    cmd_ports = []
    pubs = component_states.get_state({"component": "collector"})
    if pubs is not None:
        for k, v in pubs.items():
            cmd_ports.append(v["cmd_port"])
    return cmd_ports


def get_job_agg_port(jobid):
    global component_states
    state = component_states.get_state({"component": "job_agg", "jobid": jobid})
    if state is not None and "cmd_port" in state:
        return state["cmd_port"]


def get_top_level_group():
    global config
    for group_path in config["groups"].keys():
        if group_path.count("/") == 1:
            return group_path


def get_push_target(name):
    global component_states
    if name == "@TOP_STORE":
        top_group = get_top_level_group()
        top_store_state = component_states.get_state({"component": "data_store", "group": top_group})
        if top_store_state is not None and "listen" in top_store_state:
            return top_store_state["listen"]
        else:
            log.warning("get_push_target: top_group=%r" % top_group)
            log.warning("get_push_target: top_store_state=%r" % top_store_state)


def do_aggregate(jobid, zmq_context, **cfg):
    """
    Generate and send Aggregate Metrics commands according to an agg_cfg dict.
    Example cfg:
    { "push_target": "@TOP_STORE",
      "interval": 120,
      "agg_type": "avg",
      "ttl": 120,
      "agg_metric_name": "%(metric)s_%(agg_method)s",
      "metrics": ["load_one"] }
    """
    #cfg = copy.copy(agg_cfg)
    log.debug("do_aggregate jobid=%s cfg=%r" % (jobid, cfg))
    push_target_uri = cfg["push_target"]
    if push_target_uri.startswith("@"):
        push_target_uri = get_push_target(push_target_uri)
        cfg["push_target"] = push_target_uri
    if push_target_uri is None:
        log.error("push_target could not be resolved for agg_cfg=%r" % cfg)
        return None
    jagg_port = get_job_agg_port(jobid)
    if jagg_port is None:
        log.error("job_agg for jobid %s not found." % jobid)
        return None
    result = send_rpc(zmq_context, jagg_port, "agg", **cfg)


def make_timers(jobid):
    """
    Create one timer for each aggregator config. An aggregator config can
    trigger the aggregation of multiple metrics.
    """
    global aggregate, jagg_timers, scheduler, zmq_context

    timers = []
    for cfg in aggregate:
        if cfg["agg_class"] == "job":
            interval = cfg["interval"]
            t = RepeatEvent(scheduler, interval, do_aggregate, *[jobid, zmq_context], **cfg)
            timers.append(t)
    jagg_timers[jobid] = timers


def msg_tag_num_hosts(msg):
    "Determine the number of hosts a tag message is supposed to match."
    if "H" not in msg:
        return 0
    #if not isinstance(msg["H"], dict) or "s" not in msg["H"]:
    #    log.error("Unexpected tag message: %r" % msg)
    #    return 0
    matchexp = msg["H"]
    if matchexp.startswith("RE:"):
        return len(matchexp.lstrip("RE:").split("|"))
    return 1


def create_job_agg_instance(jobid):
    global me_rpc, component_states

    jagg = component_states.get_state({"component": "job_agg", "jobid": jobid})
    if jagg is not None and len(jagg) > 0:
        log.info("create_job_agg_instance: jobid=%s already exists. jagg=%r" % (jobid, jagg))
        return
    msgbus_arr = ["--msgbus %s" % cmd_port for cmd_port in get_collectors_cmd_ports()]
    log.info("create_job_agg_instance: jobid=%s" % jobid)
    component_states.start_component("job_agg", "/universe", jobid=jobid, __CALLBACK=make_timers, __CALLBACK_ARGS=[jobid],
                                     dispatcher=me_rpc, msgbus_opts=" ".join(msgbus_arr))
    # wait for component to appear?


def remove_job_agg_instance(jobid):
    global zmq_context, component_states, jagg_timers

    if jobid in jagg_timers:
        # kill timer if it exists
        for timer in jagg_timers[jobid]:
            timer.stop()
    jagg = component_states.get_state({"component": "job_agg", "jobid": jobid})
    if jagg is not None:
        log.info("remove_job_agg_instance: jobid=%s" % jobid)
        component_states.kill_component("job_agg", "/universe", jobid=jobid)
        # TODO: sending quit message should happen in kill_component
        #send_agg_command(zmq_context, jagg["listen"], "quit")
        # TODO: notify collectors and unsubscribe disappearing jobid


def remove_all_job_agg_instances():
    pass


def relay_to_collectors(context, cmd, msg):
    """
    Instantiate a job_agg component when new job tags are defined,
    kill job_agg component when tag is removed (i.e. job has finished).
    Relay msg to collector components RPC ports.
    """
    global config
    if cmd == "add_tag" and msg["TAG_KEY"] == "J":
        jobid = msg["TAG_VALUE"]
        nhosts = msg_tag_num_hosts(msg)
        if nhosts >= config["services"]["job_agg"]["min_nodes"]:
            create_job_agg_instance(jobid)
    elif cmd == "remove_tag" and msg["TAG_KEY"] == "J":
        jobid = msg["TAG_VALUE"]
        remove_job_agg_instance(jobid)
    elif cmd == "reset_tags":
        pass
        #remove_all_job_agg_instances()

    result = {}
    for cmd_port in get_collectors_cmd_ports():
        log.info("relaying '%s' msg to '%s'" % (cmd, cmd_port))
        result[cmd_port] = send_rpc(context, cmd_port, cmd, **msg)

    return result


def make_timers_and_save(msg, component_states, state_file):
    global jagg_timers
    log.debug("make_timers_and_save")
    if "component" in msg and msg["component"] == "job_agg":
        jobid = msg["jobid"]
        if jobid not in jagg_timers:
            log.info("make_timers(%s)" % jobid)
            make_timers(jobid)
    component_states.save_state(msg, state_file)


def start_fixups(program_restart=False, program_start=False):
    """
    -

    :param program_restart:
    :param program_start:

    :return: True if successful, False otherwise
    """

    global config, job_list, zmq_context, component_states

    num_collectors = 0
    num_collectors_started = 0
    for group_path in config["groups"]:
        group = group_name(group_path)
        pub = component_states.get_state({"component": "collector", "group": group_path})
        if pub == {} or "outdated!" in pub:
            if "outdated!" in pub:
                component_states.del_state({"component": "collector", "group": group_path})
            component_states.start_component("collector", group_path, statefile="/tmp/state.agg_collector_%s" % group,
                            dispatcher=me_rpc)
            num_collectors_started += 1
        num_collectors += 1

    if not component_states.component_wait_timeout("collector", num_collectors, timeout=180):
        return False, "timeout when waiting for collectors startup!"

    if program_restart:
        #
        # fixup subscriptions on collectors
        #
        for cmd_port in get_collectors_cmd_ports():
            log.info("asking collector '%s' to reset subscriptions" % cmd_port)
            send_rpc(zmq_context, cmd_port, "reset_subs")

    msgbus_arr = ["--msgbus %s" % cmd_port for cmd_port in get_collectors_cmd_ports()]
    for group_path in config["groups"]:
        group = group_name(group_path)
        mong = component_states.get_state({"component": "data_store", "group": group_path})
        if mong == {} or "outdated!" in mong:
            if "outdated!" in pub:
                component_states.del_state({"component": "data_store", "group": group_path})
            log.info("starting data store for group '%s'" % group)
            component_states.start_component("data_store", group_path,
                                             statefile="/tmp/state.data_store_%s" % group,
                                             dispatcher=me_rpc, msgbus_opts=" ".join(msgbus_arr))
        else:
            log.info("asking data store for group '%s' to resubscribe" % mong["group"])
            send_rpc(zmq_context, mong["cmd_port"], "resubscribe")


    #
    # and now the job aggregators
    #
    # TODO: do this more smartly, such that the entire function can be used for restarting components that failed.
    #
    # when we are not restarting, we just look after job_aggs that have failed and restart them
    # when we are restarting, we need the whole shebang on new and finished jobs and retagging
    # when collectors were restarted, subscribers need to resubscribe
    #
    for cmd_port in get_collectors_cmd_ports():
        log.info("asking collector '%s' to reset tags" % cmd_port)
        send_rpc(zmq_context, cmd_port, "reset_tags")

    jaggs = component_states.get_state({"component": "job_agg"})
    for jobid, jcomp in jaggs.items():
        if "outdated!" in jcomp or "exited!" in jcomp:
            remove_job_agg_instance(jobid)
            # what if this process has crashed?

    if program_start or program_restart:
        log.info("fixup job_aggs: %r" % job_list)
        for j in job_list:
            jobid = j["name"]
            if "cnodes" not in j:
                log.warning("fixup job_aggs: skipping job without nodes! jobid=%s" % jobid)
                continue
            log.info("fixup: jobid %s" % jobid)
            if jobid not in jaggs:
                # start aggregators for new jobs
                # these will subscribe themselves to collectors
                if len(j["cnodes"]) >= config["services"]["job_agg"]["min_nodes"]:
                    create_job_agg_instance(jobid)
            else:
                # old job_aggs
                # ask these ones to re-subscribe
                cmd_port = jaggs[jobid]["cmd_port"]
                send_rpc(zmq_context, cmd_port, "resubscribe")
                # create timers for already running job_aggs
                make_timers(jobid)

            # add tagging for running jobs
            nodes = j["cnodes"]
            if len(nodes) >= config["services"]["job_agg"]["min_nodes"]:
                if len(nodes) == 1:
                    nodesmatch = nodes[0]
                else:
                    nodesmatch = "RE:^(%s)$" % "|".join(nodes)
                for cmd_port in get_collectors_cmd_ports():
                    log.info("adding tag for job '%s' on collector '%s'" % (jobid, cmd_port))
                    send_rpc(zmq_context, cmd_port, "add_tag", TAG_KEY="J", TAG_VALUE=jobid, H=nodesmatch)


def aggmon_control(argv):

    global component_states, scheduler, zmq_context, me_rpc, job_list
    
    ap = argparse.ArgumentParser()
    ap.add_argument('-C', '--cmd-port', default="tcp://0.0.0.0:5558", action="store", help="RPC command port")
    ap.add_argument('-c', '--config', default="../config.d", action="store", help="configuration directory")
    ap.add_argument('-l', '--log', default="info", action="store", help="logging: info, debug, ...")
    ap.add_argument('-S', '--state-file', default="agg_control.state", action="store", help="file to store state")
    ap.add_argument('-k', '--kill', default=False, action="store_true", help="kill components that were left running")
    ap.add_argument('-q', '--quick', default=False, action="store_true",
                    help="kill components that were left running quickly, without waiting for 70s")
    ap.add_argument('-w', '--wait', default=False, action="store_true", help="wait for 70s even if no state file was found")
    ap.add_argument('-v', '--verbose', type=int, default=0, action="store", help="verbosity")
    pargs = ap.parse_args(argv)

    log_level = eval("logging."+pargs.log.upper())
    FMT = "%(asctime)s %(levelname)-5.5s [%(threadName)s][%(filename)s:%(lineno)d] %(message)s"
    logging.basicConfig( stream=sys.stderr, level=log_level, format=FMT )

    load_config(pargs.config)

    zmq_context = zmq.Context()

    me_addr = zmq_own_addr_for_uri("tcp://8.8.8.8:10000")

    rpc = RPCThread(zmq_context, listen=pargs.cmd_port)

    scheduler = Scheduler()
    scheduler.start()

    me_rpc = "tcp://%s:%d" % (me_addr, rpc.port)
    component_states = ComponentStatesRepo(config, me_rpc, zmq_context)
    log.info("created component_states_repo")

    rpc.register_rpc("set_component_state", component_states.set_state,
                     post=make_timers_and_save, post_args=[component_states, pargs.state_file])
    rpc.register_rpc("del_component_state", component_states.del_state,
                     post=component_states.save_state, post_args=[pargs.state_file])
    rpc.register_rpc("get_component_state", component_states.get_state)
    log.info("registered rpcs")

    if not pargs.kill:
        # connect to the top level database. the one which stores the job lists
        dbconf = config["database"]
        store = MongoDBJobList(host_name=dbconf["dbhost"], port=None, db_name=dbconf["jobdbname"],
                               username=dbconf["user"], password=dbconf["password"])

    # TODO: add smart starter of components: check/load saved state, query a resend,
    #       kill and restart if no update?
    res = component_states.load_state(pargs.state_file, mode = (pargs.kill and pargs.quick) and "kill" or "keep")
    log.info("component_states_load returned %r" % res)

    if pargs.kill:
        if res is not None or pargs.wait:
            if not pargs.quick:
                log.info("... waiting 70 seconds for state messages to come in ...")
                rpc.start()
                time.sleep(70)
            log.info("killing components that were found running...")
            component_states.kill_components(["collector", "data_store", "job_agg"])
            time.sleep(10)
            component_states.save_state(None, pargs.state_file)
        os._exit(0)
        # not sure why the sys.exit(0) does not work here
        sys.exit(0)

    log.info("starting rpc...")
    rpc.start()

    #
    # start missing components
    #
    if res is not None:
        log.info("Restarting: waiting 70 seconds for state messages to come in ...")
        time.sleep(70)
    job_list = []
    for j in store.find():
        job_list.append(j)
    start_fixups(program_restart=((res is not None) and True or False),
                 program_start=((res is None) and True or False))

    rpc.register_rpc("add_tag", lambda x: relay_to_collectors(zmq_context, "add_tag", x),
                     post=component_states.save_state, post_args=[pargs.state_file])
    rpc.register_rpc("remove_tag", lambda x: relay_to_collectors(zmq_context, "remove_tag", x),
                     post=component_states.save_state, post_args=[pargs.state_file])
    rpc.register_rpc("reset_tags", lambda x: relay_to_collectors(zmq_context, "reset_tags", x),
                     post=component_states.save_state, post_args=[pargs.state_file])
    rpc.register_rpc("show_tags", lambda x: relay_to_collectors(zmq_context, "show_tags", x))
    rpc.register_rpc("show_subs", lambda x: relay_to_collectors(zmq_context, "show_subs", x))


    #time.sleep(30)
    #kill_component("collector", "universe")
    #kill_component("data_store", "universe")

    #
    # TODO: loop and restart failed components...?
    #
    while True:
        try:
            rpc.join(0.1)
            if not rpc.isAlive():
                break
        except Exception as e:
            log.error("main thread exception: %r" % e)
            break
    print "THE END"

if __name__ == "__main__":
    aggmon_control(sys.argv)
