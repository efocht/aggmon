#!/usr/bin/python

import argparse
import copy
import json
import logging
import os
import pdb
import subprocess
import sys
import time
import zmq
from agg_rpc import send_rpc, zmq_own_addr_for_uri, RPCThread
from agg_job_command import send_agg_command
from repeat_timer import RepeatTimer
# Path Fix
sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), "../")))
from metric_store.mongodb_store import *


log = logging.getLogger( __name__ )
__doc__ = """
agg_control controls all aggmon related components, which are now separate processes
potentially running on separate hosts. It knows the entire administrative hierarchy
and orchestrates their interactions by calling them with appropriate arguments, such
that the components get linked with each other according to the hierarchy.

For each administrative (monitoring) group agg_control will start:
- one agg_pub_sub instance. This instance waits for ZMQ PUSH messages with metric metadata or
  metric values. It handles subscriptions from other components like data_store or job aggregators.
- one data_store instance. data_store instances will subscribe to the metric messages
  published by their corresponding agg_pub_sub instances.

agg_control will receive tagger requests which it will pass on to all agg_pub_sub instances.
When job tagging requests are received, an add_tag will start an instance of the job aggregator
responsible for this particular job and tell it where to subscribe (all agg_pub_sub instances).
A remove_tag request for a jobid will kill the job aggregator instance.

Job aggregators will (for now) receive periodic requests for aggregation which agg_control will
push to them. The configuration, which metrics shall be aggregated and how, must be still designed.

"""



config = {
    "groups": {
        "/universe": {
            "job_agg_nodes": ["localhost"],
            "data_store_nodes" : ["localhost"],
            "collector_nodes" : ["localhost"]
        }
    },
    "services": {
        "collector": {
            "cwd": os.getcwd(),
            "cmd": "python agg_collector.py --cmd-port %(cmdport)s --listen %(listen)s " + \
                   "--group %(group_path)s --state-file %(statefile)s --dispatcher %(dispatcher)s",
            "cmdport_range": "5100-5199",
            "component_key": ["group", "host"],
            "listen_port_range": "5262",
            "logfile": "/tmp/%(service)s_%(group)s.log"
        },
        "data_store": {
            "cwd": os.getcwd(),
            "cmd": "python data_store.py --cmd-port %(cmdport)s --listen %(listen)s " + \
                   "--dbname \"%(dbname)s\" --host \"%(dbhost)s\"" + \
                   "--group %(group_path)s --dispatcher %(dispatcher)s %(msgbus_opts)s",
            "cmdport_range": "5100-5199",
            "component_key":  ["group", "host"],
            "listen_port_range": "5200-5299",
            "logfile": "/tmp/%(service)s_%(group)s.log"
        },
        "job_agg": {
            "cwd": os.getcwd(),
            "cmd": "python agg_job_agg.py --cmd-port %(cmdport)s --listen %(listen)s " + \
                   "--jobid %(jobid)s --dispatcher %(dispatcher)s %(msgbus_opts)s",
            "cmdport_range": "5000-5999",
            "component_key":  ["jobid"],
            "listen_port_range": "5300-5999",
            "logfile": "/tmp/%(service)s_%(jobid)s.log"
        }
    },
    "database": {
        "dbname": "metricdb",
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


# cmd : "agg"
# metric : metric that should be aggregated
# agg_metric : aggregated metric name
# push_target : where to push the aggregated metric to. Can be the agg_collector
#               of the own group or one on a higher level or the mongo store.
# agg_type : aggregation type, i.e. min, max, avg, sum, worst, quant10
# ttl : (optional) time to live for metrics, should filter out old/expired metrics
# args ... : space for further aggregator specific arguments

aggregate = {
    "job": [
        { "push_target": "@TOP_STORE",
          "interval": 120,
          "agg_type": "avg",
          "ttl": 120,
          "agg_metric_name": "%(metric)s_%(agg_type)s",
          "metrics": ["load_one"]
      }
    ]
}


# hierarchy: component -> group/host
component_state = {}
component_start_cb = {}
component_kill_cb = {}

# job_agg timers indexed by jobid
jagg_timers = {}

# own (dispatcher) rpc port
me_rpc = ""

# ZeroMQ context (global)
zmq_context = None

# list of running jobs, as fresh as the mongodb collection
job_list = []


def component_key(keys, kwds):
    key = []
    for k in keys:
        if k in kwds:
            key.append(kwds[k])
        elif len(key) > 0:
            return ":".join(key) + ":"
    return ":".join(key)


def check_output(*popenargs, **kwargs):
    process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
    output, unused_err = process.communicate()
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        error = subprocess.CalledProcessError(retcode, cmd)
        error.output = output
        raise error
    return output

# trick for adding check_output on python < 2.7
try:
    subprocess.check_output
except:
    subprocess.check_output = check_output

def group_name(group):
    return group.lstrip("/").replace("/", "_")

def start_component(service, group_path, __CALLBACK=None, __CALLBACK_ARGS=[], **kwds):
    global config, component_start_cb

    if group_path not in config["groups"]:
        log.error("start_component: group '%s' not found in configuration!" % group_path)
        return False
    if service not in config["services"]:
        log.error("start_component: service '%s' not found in configuration!" % service)
        return False
    group = group_name(group_path)
    nodes_key = "%s_nodes" % service
    if nodes_key not in config["groups"][group_path]:
        log.error("start_component: '%s' not found in configuration of group '%s'!" % (nodes_key, group_path))
        return False
    nodes = config["groups"][group_path][nodes_key]
    assert(isinstance(nodes, list))
    locals().update(kwds)
    if "database" in config:
        if isinstance(config["database"], dict):
            locals().update(config["database"])
    svc_info = config["services"][service]
    cwd = svc_info["cwd"]
    cmd = svc_info["cmd"]
    if "cmdport_range" in svc_info:
        cmdport = "tcp://0.0.0.0:%s" % svc_info["cmdport_range"]
    if "listen_port_range" in svc_info:
        listen = "tcp://0.0.0.0:%s" % svc_info["listen_port_range"]
    if "logfile" in svc_info:
        logfile = svc_info["logfile"] % locals()
    state_file = "/tmp/state_%(service)s_%(group)s" % locals()
    # register callback
    if __CALLBACK is not None:
        key = service + ":" + component_key(config["services"][service]["component_key"], kwds)
        component_start_cb[key] = {"cb": __CALLBACK, "args": __CALLBACK_ARGS}
    for host in nodes:
        try:
            cmd = cmd % locals()
            exec_cmd = config["global"]["remote_cmd"] % locals()
            log.info("starting subprocess: %s" % exec_cmd)
            out = subprocess.check_output(exec_cmd, stderr=subprocess.STDOUT, shell=True)
            log.info("output: %s" % out)
            break
        except Exception as e:
            log.error("subprocess error '%r'" % e)
            log.error("subprocess error when running '%s' : '%r'" % (exec_cmd, e))
            # trying the next node, if any


def kill_component(service, group_path, __CALLBACK=None, __CALLBACK_ARGS=[], METHOD="msg", **kwds):
    global config, component_kill_cb, zmq_context, pargs

    msg = {"component": service, "group": group_path}
    msg.update(kwds)
    res = False

    group = group_name(group_path)
    state = get_component_state(msg)
    if state is None:
        log.warning("component '%s' state not found. Don't know how to kill it." % service)
        return False
    if METHOD == "msg":
        if "cmd_port" in state:
            # send "quit" cmd over RPC
            reply = send_rpc(zmq_context, state["cmd_port"], "quit")
            if reply is not None:
                res = True
        elif "listen" in state:
            # send "quit" cmd over PULL port
            send_agg_command(zmq_context, state["listen"], "quit")
            res = True
    else:
        # kill process using the remembered pid. This could be dangerous as we could kill another process.
        try:
            exec_cmd = config["global"]["remote_kill"] % state
            out = subprocess.check_output(exec_cmd, stderr=subprocess.STDOUT, shell=True)
            #res = del_component_state(msg)
            send_rpc(zmq_context, pargs.cmd_port, "del_component_state", **msg)
        except Exception as e:
            log.error("subprocess error when running '%s' : '%r'" % (exec_cmd, e))
            res = False
    return res


def set_component_state(msg):
    global component_state, config, component_start_cb

    log.debug("set_component_state: msg %r" % msg)
    if "component" not in msg:
        log.error("set_component_state message has no component!? %r" % msg)
        # TODO raise ComponentMsgError
        return None
    component = msg["component"]
    if component not in component_state:
        component_state[component] = {}
    # now make a meaningful minimal unique key
    key = component_key(config["services"][component]["component_key"], msg)
    started = None
    if key in component_state[component]:
        log.info("set_component_state: updating state for %s %s" % (component, key))
        started = component[component][key]["started"]
    else:
        log.info("set_component_state: setting state for %s %s" % (component, key))
    msg["last_update"] = time.time()
    if started is not None:
        if started != msg["started"]:
            msg["restart!"] = True
    component_state[component][key] = msg
    cbkey = component + ":" + key
    if cbkey in component_start_cb:
        cb = component_start_cb[cbkey]["cb"]
        args = component_start_cb[cbkey]["args"]
        try:
            cb(*args)
        except Exception as e:
            log.error("start_cb error: %r" % e)
        del component_start_cb[cbkey]


def get_component_state(msg):
    global component_state, config

    log.debug("get_component_state: msg %r" % msg)
    if "component" not in msg:
        return component_state
    component = msg["component"]
    if component not in component_state:
        return None
    key = component_key(config["services"][component]["component_key"], msg)
    if len(key) > 0:
        if key in component_state[component]:
            return component_state[component][key]
        else:
            # try to match for it
            for k in component_state[component].keys():
                if k.startswith(key):
                    return component_state[component][k]
    else:
        # return all components of this type
        return component_state[component]
        

def del_component_state(msg):
    global component_state, config, component_kill_cb

    log.info("del_component_state: msg %r" % msg)
    if "component" not in msg:
        return False
    component = msg["component"]
    if component not in component_state:
        return False
    key = component_key(config["services"][component]["component_key"], msg)
    if len(key) > 0:
        fullkey = None
        if key in component_state[component]:
            fullkey = key
        else:
            # try to match for it
            for k in component_state[component].keys():
                if k.startswith(key):
                    fullkey = k
                    break
        if fullkey is not None:
            log.info("del_component_state: deleting state for '%s' -> '%s'" % (component, fullkey))
            del component_state[component][fullkey]
            # callback handling
            cbkey = component + ":" + fullkey
            if cbkey in component_kill_cb:
                cb = component_start_cb[cbkey]["cb"]
                args = component_start_cb[cbkey]["args"]
                try:
                    cb(*args)
                except Exception as e:
                    log.error("kill_cb error: %r" % e)
                del component_kill_cb[cbkey]

            if component_state[component] == {}:
                del component_state[component]
            return True
    return False


def get_all_pubs_cmd_ports():
    cmd_ports = []
    pubs = get_component_state({"component": "collector"})
    if pubs is not None:
        for k, v in pubs.items():
            cmd_ports.append(v["cmd_port"])
    return cmd_ports


def get_job_agg_port(jobid):
    state = get_component_state({"component": "job_agg", "jobid": jobid})
    if state is not None and "listen" in state:
        return state["listen"]


def get_top_level_group():
    global config
    for group_path in config["groups"].keys():
        if group_path.count("/") == 1:
            return group_path


def get_push_target(name):
    if name == "@TOP_STORE":
        top_group = get_top_level_group()
        top_store_state = get_component_state({"component": "data_store", "group": top_group})
        if top_store_state is not None and "listen" in top_store_state:
            return top_store_state["listen"]


def do_aggregate(jobid, agg_cfg):
    """
    Generate and send Aggregate Metrics commands according to an agg_cfg dict.
    Example cfg:
        { "push_target": "@TOP_STORE",
          "interval": 120,
          "agg_type": "avg",
          "ttl": 120,
          "agg_metric_name": "%(metric)s_%(agg_type)s",
          "metrics": ["load_one"]
      }
    """
    log.debug("do_aggregate jobid=%s cfg=%r" % (jobid, agg_cfg))
    push_target_uri = get_push_target(agg_cfg["push_target"])
    if push_target_uri is None:
        log.error("push_target could not be resolved for agg_cfg=%r" % agg_cfg)
        return None
    jagg_port = get_job_agg_port(jobid)
    if jagg_port is None:
        log.error("job_agg for jobid %s not found." % jobid)
        return None
    agg_type = agg_cfg["agg_type"]
    for metric in agg_cfg["metrics"]:
        agg_metric = agg_cfg["agg_metric_name"] % locals()
        kwds = {"metric": metric,
                "agg_metric": agg_metric,
                "agg_type": agg_type,
                "push_target": push_target_uri}
        if "ttl" in agg_cfg:
            kwds["ttl"] = agg_cfg["ttl"]
        send_agg_command(zmq_context, jagg_port, "agg", **kwds)

    
def make_timers(jobid):
    global aggregate, component_state, jagg_timers

    timers = []
    for cfg in aggregate["job"]:
        interval = cfg["interval"]
        t = RepeatTimer(interval, do_aggregate, jobid, cfg)
        timers.append(t)
    jagg_timers[jobid] = timers


def create_job_agg_instance(jobid):
    global me_rpc

    jagg = get_component_state({"component": "job_agg", "jobid": jobid})
    if jagg is not None:
        return
    msgbus_arr = ["--msgbus %s" % cmd_port for cmd_port in get_all_pubs_cmd_ports()]
    start_component("job_agg", "/universe", jobid=jobid, __CALLBACK=make_timers, __CALLBACK_ARGS=[jobid],
                    dispatcher=me_rpc, msgbus_opts=" ".join(msgbus_arr))
    # wait for component to appear?
    # create timer instance and set in component_state? can this be handled as a callback?
    


def remove_job_agg_instance(jobid):
    global zmq_context

    if jobid in jagg_timers:
        # kill timer if it exists
        for timer in jagg_timers[jobid]:
            timer.stop()
    jagg = get_component_state({"component": "job_agg", "jobid": jobid})
    if jagg is not None:
        kill_component("job_agg", "/universe", jobid=jobid)
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
    if cmd == "add_tag" and msg["TAG_KEY"] == "J":
        jobid = msg["TAG_VALUE"]
        create_job_agg_instance(jobid)
    elif cmd == "remove_tag" and msg["TAG_KEY"] == "J":
        jobid = msg["TAG_VALUE"]
        remove_job_agg_instance(jobid)
    elif cmd == "reset_tags":
        pass
        #remove_all_job_agg_instances()

    for cmd_port in get_all_pubs_cmd_ports():
        log.info("relaying msg to '%s'" % cmd_port)
        send_rpc(context, cmd_port, cmd, **msg)


def request_resend(state):
    global zmq_context

    res = False
    if "cmd_port" in state:
        # send "quit" cmd over RPC
        reply = send_rpc(zmq_context, state["cmd_port"], "resend_state")
        if reply is not None:
            res = True
    elif "listen" in state:
        # send "quit" cmd over PULL port
        send_agg_command(zmq_context, state["listen"], "resend_state")
        res = True
    return res


def load_state(name):
    try:
        fp = open(name)
        state = json.load(fp)
        fp.close()
    except Exception as e:
        log.error("Exception in state load '%s': %r" % (name, e))
        return []
    return state


def load_component_state(name, mode="keep"):
    """
    Load component state from disk or database.
    Parameters:
    name: the file name where the state is saved
    mode: load mode: can be:
          "keep": keep running components
          "restart": restart running components
          ...?
    """
    loaded = load_state(name)
    if len(loaded) == 0 or len(loaded[0]) == 0:
        return None
    loaded_state = loaded[0]
    #
    # only mode == "keep" is implemented
    #
    # request resend of state from all components
    for component, compval in loaded_state.items():
        component_state[component] = compval
        for ckey, cstate in compval.items():
            # set the "outdated!" attribute
            # it will disappear if the component sends a component update message
            # thus it is used for marking non-working components
            component_state[component][ckey]["outdated!"] = True
            request_resend(cstate)
            time.sleep(0.05)
    # create timers for running aggregators
    if "job_agg" in loaded_state:
        for jobid, jstate in loaded_state["job_agg"].items():
            make_timers(jobid)
    return True


def save_state(name, state):
    """
    'name' is the filename where to store the state
    'state' is a list of objects to be saved
    """
    try:
        fp = open(name, "w")
        json.dump(state, fp)
        fp.close()
    except Exception as e:
        log.error("Exception in state save '%s': %r" % (name, e))
        return False
    return True


def save_state_post(msg):
    global component_state, pargs

    log.debug("save_state_post")
    save_state(pargs.state_file, [component_state])


def make_timers_and_save(msg):
    global component_state

    log.debug("make_timers_and_save")
    if "component" in msg and msg["component"] == "job_agg":
        jobid = msg["jobid"]
        if jobid not in jagg_timers:
            make_timers(jobid)
    save_state_post(msg)


def start_missing_components():
    global job_list, zmq_context

    for group_path in config["groups"]:
        group = group_name(group_path)
        pub = get_component_state({"component": "collector", "group": group_path})
        if pub is None or "outdated!" in pub:
            start_component("collector", group_path, statefile="/tmp/state.agg_collector_%s" % group,
                            dispatcher=me_rpc)
    while "collector" not in component_state:
        time.sleep(0.1)
    time.sleep(2)

    msgbus_arr = ["--msgbus %s" % cmd_port for cmd_port in get_all_pubs_cmd_ports()]
    for group_path in config["groups"]:
        group = group_name(group_path)
        mong = get_component_state({"component": "data_store", "group": group_path})
        if mong is None or "outdated!" in mong:
            start_component("data_store", group_path, statefile="/tmp/state.data_store_%s" % group,
                            dispatcher=me_rpc, msgbus_opts=" ".join(msgbus_arr))

    #
    # fixup subscriptions on collectors
    #
    for cmd_port in get_all_pubs_cmd_ports():
        log.info("asking '%s' to reset subscriptions and tags" % cmd_port)
        send_rpc(zmq_context, cmd_port, "reset_tags")
        send_rpc(zmq_context, cmd_port, "reset_subs")

    #
    # and now the job aggregators
    #
    jaggs = get_component_state({"component": "job_agg"})
    for jobid, jcomp in jaggs.items():
        if "outdated!" in jcomp:
            remove_job_agg_instance(jobid)
            # what if this process has crashed?
    for j in job_list:
        jobid = j["name"]
        if jobid not in jaggs:
            # these will subscribe
            create_job_agg_instance(jobid)
        else:
            # ask these ones to re-subscribe
            cmd_port = jaggs[jobid]["cmd_port"]
            send_rpc(zmq_context, cmd_port, "resubscribe")

        # add tagging for running jobs
        nodes = j["cnodes"]
        if len(nodes) == 1:
            nodesmatch = nodes[0]
        else:
            nodesmatch = "RE:^(%s)$" % "|".join(nodes)
        for cmd_port in get_all_pubs_cmd_ports():
            send_rpc(zmq_context, cmd_port, "add_tag", TAG_KEY="J", TAG_VALUE=jobid, J=nodesmatch)

    #
    # ask data stores to resubscribe
    #
    stores = get_component_state({"component": "data_store"})
    for skey, state in stores.items():
        cmd_port = state["cmd_port"]
        send_rpc(zmq_context, cmd_port, "resubscribe")



def kill_components():
    for group_path in config["groups"]:
        for comp_type in ("collector", "data_store", "job_agg"):
            c = get_component_state({"component": comp_type, "group": group_path})
            if c is not None:
                if "component" in c:
                    kill_component(comp_type, group_path, METHOD="kill")
                else:
                    log.info("components: %r" % c)
                    for jobid, jagg in c.items():
                        kill_component(comp_type, group_path, jobid=jobid, METHOD="kill")


if __name__ == "__main__":
    #global me_rpc, zmq_context

    ap = argparse.ArgumentParser()
    ap.add_argument('-C', '--cmd-port', default="tcp://0.0.0.0:5556", action="store", help="RPC command port")
    ap.add_argument('-c', '--config', default="", action="store", help="configuration file")
    ap.add_argument('-l', '--log', default="info", action="store", help="logging: info, debug, ...")
    ap.add_argument('-S', '--state-file', default="agg_control.state", action="store", help="file to store state")
    ap.add_argument('-k', '--kill', default=False, action="store_true", help="kill components that were left running")
    ap.add_argument('-v', '--verbose', type=int, default=0, action="store", help="verbosity")
    pargs = ap.parse_args()

    log_level = eval("logging."+pargs.log.upper())
    FMT = "%(asctime)s %(levelname)-5.5s [%(threadName)s][%(filename)s:%(lineno)d] %(message)s"
    logging.basicConfig( stream=sys.stderr, level=log_level, format=FMT )


    state = []
    if len(pargs.state_file) > 0:
        state = load_state(pargs.state_file)
        # now check running daemons, etc.

    zmq_context = zmq.Context()

    rpc = RPCThread(zmq_context, listen=pargs.cmd_port)
    rpc.start()

    rpc.register_rpc("set_component_state", set_component_state, post=make_timers_and_save)
    rpc.register_rpc("del_component_state", del_component_state, post=save_state_post)
    rpc.register_rpc("get_component_state", get_component_state)

    me_addr = zmq_own_addr_for_uri("tcp://8.8.8.8:10000")
    me_rpc = "tcp://%s:%d" % (me_addr, rpc.port)

    if not pargs.kill:
        # connect to the top level database. the one which stores the job lists
        dbconf = config["database"]
        store = MongoDBJobList(host_name=dbconf["dbhost"], port=None, db_name=dbconf["dbname"],
                               username=dbconf["user"], password=dbconf["password"])


    # TODO: add smart starter of components: check/load saved state, query a resend, kill and restart if no update?
    res = load_component_state(pargs.state_file)

    if res is not None and pargs.kill:
        log.info("... waiting 70 seconds for state messages to come in ...")
        time.sleep(70)
        log.info("killing components that were found running...")
        kill_components()
        time.sleep(10)
        sys.exit(0)

    #
    # start missing components

    #
    if res is not None:
        log.info("Restarting: waiting 70 seconds for state messages to come in ...")
        time.sleep(70)
    job_list = store.find()
    start_missing_components()

    rpc.register_rpc("add_tag", lambda x: relay_to_collectors(zmq_context, "add_tag", x), post=save_state_post)
    rpc.register_rpc("remove_tag", lambda x: relay_to_collectors(zmq_context, "remove_tag", x), post=save_state_post)
    rpc.register_rpc("reset_tags", lambda x: relay_to_collectors(zmq_context, "reset_tags", x), post=save_state_post)
    #rpc.register_rpc("show_tags", local_show_tags)


    # TODO: deal with job aggs
    job_list = store.find()



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


