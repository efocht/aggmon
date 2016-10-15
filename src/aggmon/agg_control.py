#!/usr/bin/python2

import argparse
import copy
import json
import logging
import os
import os.path
import pdb
import signal
import sys
import time
import traceback
import zmq
from agg_component import ComponentStatesRepo, group_name, ComponentDeadError
from agg_rpc import send_rpc, zmq_own_addr_for_uri, RPCThread, RPC_TIMEOUT, RPCNoReplyError
from config import Config
from scheduler import Scheduler
from repeat_event import RepeatEvent
from res_mngr import PBSNodes
# Path Fix
sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.dirname(
                os.path.realpath(__file__)), "../")))
#from metric_store.mongodb_store import *


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

# list of running jobs, as fresh as the db collection
job_list = {}

# controls main program loop
main_stopping = False

##
# default configuration data
##
config = {}


def create_pidfile(fname):
    try:
        f = open(fname, "w")
        f.write(str(os.getpid())+"\n")
        f.close()
    except:
        raise Exception("Cannot create pid file '%s'" % (fname))

def clean_pidfile(fname):
    if os.path.exists(fname):
        os.remove(fname)

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
    for group_path in config.get("/groups").keys():
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
    global config, jagg_timers, scheduler, zmq_context

    timers = []
    for cfg in config.get("/aggregate"):
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


def cmd_to_collectors(context, cmd, msg, restrict=None):
    result = {}
    failed = {}
    if isinstance(restrict, (list, set)):
        cmd_ports = restrict
    else:
        cmd_ports = get_collectors_cmd_ports()
    for cmd_port in cmd_ports:
        log.info("sending '%s' %r msg to '%s'" % (cmd, msg, cmd_port))
        try:
            result[cmd_port] = send_rpc(context, cmd_port, cmd, **msg)
        except RPCNoReplyError as e:
            log.warning("%r" % e)
            failed[cmd_port] = True
    return (result, failed,)


def remove_job_agg_instance(jobid):
    global zmq_context, component_states, jagg_timers

    if jobid in jagg_timers:
        # kill timer if it exists
        for timer in jagg_timers[jobid]:
            try:
                timer.stop()
            except Exception as e:
                log.warning("time.stop() exception: %r" % e)
        del jagg_timers[jobid]
    jagg = component_states.get_state({"component": "job_agg", "jobid": jobid})
    if jagg is None or len(jagg) == 0:
        log.info("remove_job_agg_instance: jobid=%s not found in component_states" % jobid)
        return

    log.info("remove_job_agg_instance: jobid=%s" % jobid)
    component_states.kill_component("job_agg", "/universe", jobid=jobid)
    #
    # notify collectors and unsubscribe disappearing jobid !!!!!!!!!!!!!!!!!!!!
    #
    log.info("remove_job_agg_instance: unsubscribing jobid=%s" % jobid)
    res, fails = cmd_to_collectors(zmq_context, "unsubscribe", {"TARGET": jagg["listen"]})
    log.info("remove_job_agg_instance: unsubscribe done")
    return res


def remove_all_job_agg_instances():
    pass


def relay_to_collectors(context, cmd, msg):
    """
    Instantiate a job_agg component when new job tags are defined,
    kill job_agg component when tag is removed (i.e. job has finished).
    Relay msg to collector components RPC ports.
    """
    global config
    jobid = "-"
    if cmd == "add_tag" and msg["TAG_KEY"] == "J":
        jobid = msg["TAG_VALUE"]
        nhosts = msg_tag_num_hosts(msg)
        if nhosts >= config.get("/services/job_agg/min_nodes"):
            create_job_agg_instance(jobid)
    elif cmd == "remove_tag" and msg["TAG_KEY"] == "J":
        jobid = msg["TAG_VALUE"]
        remove_job_agg_instance(jobid)
    elif cmd == "reset_tags":
        pass
        #remove_all_job_agg_instances()
    return cmd_to_collectors(context, cmd, msg)


def make_timers_and_save(msg, component_states, state_file):
    global jagg_timers
    log.debug("make_timers_and_save")
    if "component" in msg and msg["component"] == "job_agg":
        jobid = msg["jobid"]
        if jobid not in jagg_timers:
            log.info("make_timers(%s)" % jobid)
            make_timers(jobid)
    component_states.save_state(msg, state_file)


def check_restart_collectors(config, component_states):
    #
    # Handle collectors
    #
    num_collectors = 0
    coll_started = []
    for group_path in config.get("/groups"):
        group = group_name(group_path)
        pub = component_states.get_state({"component": "collector", "group": group_path})
        if pub == {} or "outdated!" in pub:
            if "outdated!" in pub:
                component_states.kill_component("collector", group_path)
            component_states.start_component("collector", group_path, statefile="/tmp/state.agg_collector_%s" % group,
                                             dispatcher=me_rpc)
            coll_started.append(group_path)
        num_collectors += 1
    return num_collectors, coll_started


def check_restart_data_stores(config, component_states):
    #
    # Handle data stores
    #
    msgbus_arr = ["--msgbus %s" % cmd_port for cmd_port in get_collectors_cmd_ports()]
    for group_path in config.get("/groups"):
        group = group_name(group_path)
        mong = component_states.get_state({"component": "data_store", "group": group_path})
        if mong == {} or "outdated!" in mong:
            if "outdated!" in mong:
                component_states.kill_component("data_store", group_path)
            log.info("starting data store for group '%s'" % group)
            component_states.start_component("data_store", group_path,
                                             statefile="/tmp/state.data_store_%s" % group,
                                             dispatcher=me_rpc, msgbus_opts=" ".join(msgbus_arr))
        else:
            log.info("asking data store for group '%s' to resubscribe" % mong["group"])
            send_rpc(zmq_context, mong["cmd_port"], "resubscribe")

def get_job_list(config):
    from res_mngr import PBSNodes
    # TODO: where do we get host/port from?
    pbs = PBSNodes(host=config.get("/resource_manager/master"),
                   port=config.get("/resource_manager/ssh_port"),
                   pull_state_cmd=config.get("/resource_manager/pull_state_cmd"))
    pbs.update()
    return pbs.job_nodes
    #return set(pbs.job_nodes.keys())

def get_current_job_tags():
    """
    Ask all collectors for their job tags. Merge these into one set.
    """
    global zmq_context

    tags = set()
    #tags_resp = send_rpc(zmq_context, me_rpc, "show_tags")
    tags_resp, failed = cmd_to_collectors(zmq_context, "show_tags", {})
    if len(failed) > 0:
        log.warning("Collectors %s failed for 'show_tags' command" % ", ".join(failed.keys()))
    if len(tags_resp) == 0:
        raise ComponentDeadError("all collectors")
    for collector in tags_resp.keys():
        for tag in tags_resp[collector].keys():
            if tag.startswith("J:"):
                tags.add(tag.lstrip("J:"))
    return tags

def component_control():
    global config, job_list, zmq_context, component_states, me_rpc

    # get list of outdated components
    outdated = component_states.check_outdated()

    num_groups = len(config.get("/groups"))
    # restart collectors, if needed
    collector_states = component_states.get_state({"component": "collector"})
    coll_started = []
    if len(collector_states) < num_groups or \
       "collector" in outdated and len(outdated["collector"]) > 0:
        num_collectors, coll_started = check_restart_collectors(config, component_states)
    else:
        num_collectors = num_groups

    if not component_states.component_wait_timeout("collector", num_collectors, timeout=180):
        log.error("timeout when waiting for collectors startup!")

    # restart data stores, if needed
    dstore_states = component_states.get_state({"component": "data_store"})
    if len(dstore_states) < num_groups or \
       "data_store" in outdated and len(outdated["data_store"]) > 0:
        check_restart_data_stores(config, component_states)

    # get fresh job_list
    fresh_job_nodes = get_job_list(config)
    fresh_joblist = set(fresh_job_nodes.keys())

    # get old job list
    try:
        current_tags = get_current_job_tags()
    except ComponentDeadError as e:
        log.error("Collector(s) didn't reply: %r" % e)
        return

    # restart outdated job aggs which actually should be running
    # and stop those which should be finished

    outdated_joblist = set()
    outdated_but_running = set()
    for state in outdated.get("job_agg", []):
        jobid = state["jobid"]
        outdated_joblist.add(jobid)
        # remove instance and unsubscribe
        remove_job_agg_instance(jobid)
        if jobid in fresh_joblist:
            # start it
            if len(fresh_job_nodes[jobid]) >= config.get("/services/job_agg/min_nodes"):
                create_job_agg_instance(jobid)
                outdated_but_running.add(jobid)
        else:
            # untag finished jobs
            relay_to_collectors(zmq_context, "remove_tag", {"TAG_KEY": "J",
                                                            "TAG_VALUE": jobid})


    log.info("current_tags    : %r" % current_tags)
    log.info("fresh_joblist   : %r" % fresh_joblist)
    if len(outdated_joblist) > 0:
        log.info("outdated_joblist: %r" % outdated_joblist)
    for jobid in (current_tags - fresh_joblist - outdated_joblist):
        # kill finished job_aggs
        remove_job_agg_instance(jobid)
        # untag finished jobs
        relay_to_collectors(zmq_context, "remove_tag", {"TAG_KEY": "J",
                                                        "TAG_VALUE": jobid})

    for jobid in (fresh_joblist - current_tags):
        # start new job_aggs
        # tag new jobs
        jnodes = fresh_job_nodes[jobid]
        if len(jnodes) == 1:
            nodesmatch = jnodes[0]
        else:
            nodesmatch = "RE:^(%s)$" % "|".join(jnodes)
        relay_to_collectors(zmq_context, "add_tag", {"H": nodesmatch,
                                                     "TAG_KEY": "J",
                                                     "TAG_VALUE": jobid})

    # TODO: handle tags on newly started collectors
    coll_started_cmd_ports = set()
    for group_path in coll_started:
        coll = component_states.get_state({"component": "collector", "group": group_path})
        coll_started_cmd_ports.add(coll["cmd_port"])

    # tags for outdated job_aggs which still need to run
    for jobid in outdated_but_running:
        jnodes = fresh_job_nodes[jobid]
        if len(jnodes) == 1:
            nodesmatch = jnodes[0]
        else:
            nodesmatch = "RE:^(%s)$" % "|".join(jnodes)
        cmd_to_collectors(zmq_context, "add_tag", {"H": nodesmatch,
                                                   "TAG_KEY": "J",
                                                   "TAG_VALUE": jobid},
                          restrict=coll_started_cmd_ports)

    # tags and subscriptions for old job aggs fresh_joblist.intersection(current_tags)
    for jobid in fresh_joblist.intersection(current_tags):
        jnodes = fresh_job_nodes[jobid]
        if len(jnodes) == 1:
            nodesmatch = jnodes[0]
        else:
            nodesmatch = "RE:^(%s)$" % "|".join(jnodes)
        cmd_to_collectors(zmq_context, "add_tag", {"H": nodesmatch,
                                                   "TAG_KEY": "J",
                                                   "TAG_VALUE": jobid},
                          restrict=coll_started_cmd_ports)
        jagg = component_states.get_state({"component": "job_agg", "jobid": jobid})
        if jagg is None or len(jagg) == 0:
            # TODO: check if we create duplicate instances here!!!
            if len(jnodes) >= config.get("/services/job_agg/min_nodes"):
                create_job_agg_instance(jobid)
        else:
            cmd_to_collectors(zmq_context, "subscribe", {"J": jobid,
                                                         "TARGET": jagg["listen"]},
                              restrict=coll_started_cmd_ports)

    # subscriptions for not restarted data_collectors
    # actually handled in data_store restart, but may be wrong


def sig_handler(signum, stack):
    global main_stopping
    log.info("Received signal %d" % signum)
    if signum in (signal.SIGINT, signal.SIGQUIT, signal.SIGHUP, signal.SIGTERM):
        main_stopping = True


def aggmon_control(argv):

    global component_states, scheduler, zmq_context, me_rpc, job_list, main_stopping, config
    
    ap = argparse.ArgumentParser()
    ap.add_argument('-C', '--cmd-port', default="tcp://0.0.0.0:5558", action="store", help="RPC command port")
    ap.add_argument('-c', '--config', default="../config.d", action="store", help="configuration directory")
    ap.add_argument('-l', '--log', default="info", action="store", help="logging: info, debug, ...")
    ap.add_argument('-L', '--loop-time', default=30, action="store", help="control loop time, default 30s")
    ap.add_argument('-S', '--state-file', default="agg_control.state", action="store", help="file to store state")
    ap.add_argument('-p', '--pid-file', default="agg_control.pid", action="store", help="file to pid")
    ap.add_argument('-k', '--kill', default=False, action="store_true", help="kill components that were left running")
    ap.add_argument('-q', '--quick', default=False, action="store_true",
                    help="kill components that were left running quickly, without waiting for 70s")
    ap.add_argument('-w', '--wait', default=False, action="store_true", help="wait for 70s even if no state file was found")
    ap.add_argument('-v', '--verbose', type=int, default=0, action="store", help="verbosity")
    pargs = ap.parse_args(argv)

    log_level = eval("logging."+pargs.log.upper())
    FMT = "%(asctime)s %(levelname)-5.5s [%(threadName)s][%(filename)s:%(lineno)d] %(message)s"
    logging.basicConfig( stream=sys.stderr, level=log_level, format=FMT )

    log.info("agg_control called with pargs: %r" % pargs)

    config = Config(config_dir=pargs.config)

    if len(config.get("/resource_manager/master")) == 0:
        log.error("No master node specified for the resource manager! Exitting!")
        sys.exit(1)

    create_pidfile(pargs.pid_file)

    for signum in (signal.SIGINT, signal.SIGQUIT, signal.SIGHUP, signal.SIGTERM):
        signal.signal(signum, sig_handler)
    
    zmq_context = zmq.Context()

    me_addr = zmq_own_addr_for_uri("tcp://8.8.8.8:10000")

    rpc = RPCThread(zmq_context, listen=pargs.cmd_port)

    #scheduler = Scheduler()
    #scheduler.start()

    me_rpc = "tcp://%s:%d" % (me_addr, rpc.port)
    component_states = ComponentStatesRepo(config, me_rpc, zmq_context)
    log.info("created component_states_repo")

    rpc.register_rpc("set_component_state", component_states.set_state,
                     post=make_timers_and_save, post_args=[component_states, pargs.state_file])
    rpc.register_rpc("del_component_state", component_states.del_state,
                     post=component_states.save_state, post_args=[pargs.state_file])
    rpc.register_rpc("get_component_state", component_states.get_state)

    #rpc.register_rpc("add_tag", lambda x: relay_to_collectors(zmq_context, "add_tag", x),
    #                 post=component_states.save_state, post_args=[pargs.state_file])
    #rpc.register_rpc("remove_tag", lambda x: relay_to_collectors(zmq_context, "remove_tag", x),
    #                 post=component_states.save_state, post_args=[pargs.state_file])
    #rpc.register_rpc("reset_tags", lambda x: relay_to_collectors(zmq_context, "reset_tags", x),
    #                 post=component_states.save_state, post_args=[pargs.state_file])

    rpc.register_rpc("show_tags", lambda x: cmd_to_collectors(zmq_context, "show_tags", x))
    
    rpc.register_rpc("reset_subs", lambda x: cmd_to_collectors(zmq_context, "reset_subs", x),
                     post=component_states.save_state, post_args=[pargs.state_file])
    
    rpc.register_rpc("show_subs", lambda x: cmd_to_collectors(zmq_context, "show_subs", x))
    log.info("registered rpcs")

    scheduler = Scheduler()
    scheduler.start()

        # connect to the top level database. the one which stores the job lists
        #dbconf = config["database"]
        #store = MongoDBJobList(host_name=dbconf["dbhost"], port=None, db_name=dbconf["jobdbname"],
        #                       username=dbconf["user"], password=dbconf["password"])

    # TODO: add smart starter of components: check/load saved state, query a resend,
    #       kill and restart if no update?
    res = component_states.load_state(pargs.state_file)
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
            if not pargs.quick:
                rpc.stop()
                rpc.join()
        clean_pidfile(pargs.pid_file)
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

    #
    # TODO: loop and restart failed components...?
    #
    while not main_stopping:
        try:
            start_time = time.time()
            component_control()
            end_time = time.time()
            sleep_time = pargs.loop_time - (end_time - start_time)
            if sleep_time > 0:
                log.info("sleeping %fs" % sleep_time)
                while not main_stopping and time.time() < start_time + pargs.loop_time:
                    time.sleep(0.5)
            else:
                log.info("component control time exceeded the loop time by %fs. Consider increasing it!" % abs(sleep_time))
        except Exception as e:
            log.error(traceback.format_exc())
            log.error("main thread exception: %r" % e)
            main_stopping = True
    print "THE END"
    rpc.stop()
    rpc.join()
    scheduler.stop()
    scheduler.join()
    clean_pidfile(pargs.pid_file)
    os._exit(0)
    #rpc.join()
    

if __name__ == "__main__":
    aggmon_control(sys.argv)
