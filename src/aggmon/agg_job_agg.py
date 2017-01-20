#!/usr/bin/python

import argparse
import logging
import sys
import re
import pdb
#from pymongo import MongoClient, ASCENDING
import signal
import threading
import time
try:
    import ujson as json
except:
    import json
import zmq
from Queue import Queue, Empty
from agg_helpers import *
from agg_mcache import MCache
from etcd_component import get_kwds, ComponentState
from etcd_rpc import *
import basic_aggregators as aggs
from config import Config, DEFAULT_CONFIG_DIR
from repeat_event import RepeatEvent
from scheduler import Scheduler


log = logging.getLogger( __name__ )
component = None
main_stopping = False


"""
Messages flow in and are put onto a queue. The messages can be:
1) Metric Value messages
2) Command messages
The later contain the "_COMMAND_" attribute and the "J" attribute (which is probably not necessary).

The value of the _COMMAND_ attribute is a dict with the command itself and arguments:

Following commands are implemented:

The "aggregate" command: triggers the aggregation of a particular metric.
Arguments:
 cmd : "agg"
 metric : metric that should be aggregated
 agg_metric : aggregated metric name
 push_target : where to push the aggregated metric to. Can be the agg_pub_sub
               of the own group or one on a higher level or the data store.
 agg_type : aggregation type, i.e. min, max, avg, sum, worst, quant10
 ttl : (optional) time to live for metrics, should filter out old/expired metrics
 args ... : space for further aggregator specific arguments

The "quit" command: terminates the current instance of the aggregator. This is
invoked when the job ends and the job tagging for it is unregistered.
 cmd : "quit"

"""

class ZMQ_Push(object):
    def __init__(self, zmq_context):
        self.zmq_context = zmq_context
        # PUSH sockets by target
        self.send_socket = {}

    def sock_connect(self, target):
        sock = self.zmq_context.socket(zmq.PUSH)
        sock.connect(target)
        self.send_socket[target] = sock

    def send(self, target, msg):
        if target not in self.send_socket:
            self.sock_connect(target)
        jmsg = json.dumps(msg)
        try:
            self.send_socket[target].send_string(jmsg, flags=zmq.NOBLOCK)
        except:
            return False
        return True


class JobAggregator(threading.Thread):
    def __init__(self, jobid, zmq_context):
        self.jobid = jobid
        self.queue = Queue()
        # every metric is stored in a separate mcache, such that we can quickly aggregate its values
        self.metric_caches = {}
        self.zmq_push = ZMQ_Push(zmq_context)
        self.stopping = False
        threading.Thread.__init__(self, name="job_agg")
        self.daemon = True

    def do_aggregate_and_send(self, cmd):
        log.debug("do_aggregate_and_send: cmd = %r" % cmd)
        metrics = cmd["metrics"]
        num_sent = 0
        if isinstance(metrics, basestring):
            if metrics.startswith("RE:"):
                metrics_match = metrics.lstrip("RE:")
                metrics = []
                for metric in self.metric_caches.keys():
                    m = re.match(metrics_match, metric)
                    if m:
                        metrics.append(metric)
            else:
                metrics = [metrics]
        for metric in metrics:
            log.debug("do_aggregate_and_send: metric = %s" % metric)
            # TODO: add regexp case
            agg_type = cmd["agg_type"]
            push_target = cmd["push_target"]
            agg_metric = cmd["agg_metric_name"] % locals()
            ttl = None
            if "ttl" in cmd:
                ttl = cmd["ttl"]
            now = time.time()

            values = []
            if metric not in self.metric_caches:
                log.debug("metric '%s' not found in metric_caches!" % metric)
                continue
            for host, val in self.metric_caches[metric].items():
                t, v = val
                if ttl is not None and t < now - ttl:
                    continue
                values.append(v)
            log.debug("calling aggregate: agg_type=%s, values=%r" % (agg_type, values))
            agg_value = aggs.aggregate(agg_type, values)
            log.debug("aggregate returned: %r" % agg_value)
            #
            # TODO: what should the "H" (host) be for an aggregated metric?
            # We set it empty, but this bears the risk to lose data because multiple jobs
            # can lead to the same (H, N, T) index in the database. The risk is limited if
            # T is a float.
            #
            metric = {"H": "", "N": agg_metric, "J": self.jobid, "T": now, "V": agg_value}
            log.debug("agg metric = %r, pushing it to %s" % (metric, push_target))
            rc = self.zmq_push.send(push_target, metric)
            if rc:
                num_sent += 1
        return num_sent

    def run(self):
        log.info( "[Started JobAggregator Thread]" )
        self.req_worker()

    def req_worker(self):
        """
        The request worker will pick a request from the Queue. If it is a metric value message,
        it will be added to the cache. If it is an aggregation command, it triggers the aggregation.
        """
        while not self.stopping:
            try:
                msg = self.queue.get(timeout=1)
            except (KeyboardInterrupt, SystemExit) as e:
                log.warning("Interrupt in thread? %r" % e)
                continue
            except Empty:
                time.sleep(0.05)
                continue
    
            try:
                #
                # do the work
                #
                if "V" in msg:
                    #log.debug("value message: %r" % msg)
                    # this is a value message
                    metric = msg["N"]
                    pc = metric.split(".")
                    if len(pc) > 2 and pc[0] == "servers":
                        metric = ".".join(pc[2:])
                    # add it to the cache
                    if metric not in self.metric_caches:
                        self.metric_caches[metric] = MCache()
                    val = (msg["T"], msg["V"],)
                    #log.debug("job_agg: inserting %r to cache" % (val,))
                    self.metric_caches[metric].set(msg["H"], val)

            except Exception as e:
                log.error( "Exception in job_agg req worker: %r, %r" % (e, msg) )

            self.queue.task_done()


def sig_handler(signum, stack):
    global main_stopping
    log.info("Received signal %d" % signum)
    if signum in (signal.SIGINT, signal.SIGQUIT, signal.SIGHUP, signal.SIGTERM):
        main_stopping = True

            
def aggmon_jobagg(argv):
    global component, main_stopping

    ap = argparse.ArgumentParser()
    ap.add_argument('-c', '--config', default=DEFAULT_CONFIG_DIR,
                    action="store", help="configuration directory")
    ap.add_argument('-H', '--hierarchy-url', default="", action="store",
                    help="position in hierarchy for this component, eg. group:/universe")
    ap.add_argument('-l', '--log', default="info", action="store",
                    help="logging: info, debug, ...")
    ap.add_argument('-L', '--listen', default="tcp://127.0.0.1:5560",
                    action="store", help="zmq pull port to listen on")
    ap.add_argument('-M', '--msgbus', default=[], action="append",
                    help="subscription port(s) for message bus. can be used multiple times.")
    ap.add_argument('-s', '--stats', default=False, action="store_true",
                    help="print statistics info")
    ap.add_argument('-v', '--verbose', type=int, default=0, action="store",
                    help="verbosity")
    pargs = ap.parse_args(argv)

    log_level = eval("logging."+pargs.log.upper())
    FMT = "%(asctime)s %(levelname)-5.5s [%(name)s][%(threadName)s] %(message)s"
    logging.basicConfig( stream=sys.stderr, level=log_level, format=FMT )

    if len(pargs.hierarchy_url) == 0:
        log.error("No hierarchy URL provided for this component. Use the -H option!")
        sys.exit(1)

    component = None

    config = Config(config_dir=pargs.config)

    if len(pargs.jobid) == 0:
        log.error("jobid argument can not be empty!")
        sys.exit(1)

    scheduler = Scheduler()
    scheduler.start()

    for signum in (signal.SIGINT, signal.SIGQUIT, signal.SIGHUP, signal.SIGTERM):
        signal.signal(signum, sig_handler)

    zmq_context = zmq.Context()
    try:
        jagg = JobAggregator(pargs.jobid, zmq_context)
    except Exception as e:
        log.error("Failed to create JobAggregator: %r" % e)
        sys.exit(1)
    jagg.start()

    # Socket to receive messages on
    receiver = zmq_context.socket(zmq.PULL)
    receiver.setsockopt(zmq.RCVHWM, 40000)
    receiver.setsockopt(zmq.RCVTIMEO, 1000)
    receiver.setsockopt(zmq.LINGER, 0)
    recv_port = socket_bind_range(receiver, pargs.listen)
    assert(recv_port is not None)

    etcd_client = EtcdClient()
    me_addr = own_addr_for_tgt("8.8.8.8")
    me_listen = "tcp://%s:%d" % (me_addr, recv_port)
    state = get_kwds(listen=me_listen, jobid=pargs.jobid)
    component = ComponentState(etcd_client, "job_agg", pargs.hierarchy_url, state=state)

    collectors_rpc_paths = []
    for cstate in component.iter_components_state(component_type="collector"):
        collectors_rpc_paths.append(cstate.value["rpc_path"])

    def aggregate_rpc(msg):
        _aggregate_rpc(**msg)

    def _aggregate_rpc(**msg):
        agg_rpcs = component.state.get("stats.agg_rpcs", 0)
        agg_rpcs += 1
        num_sent = jagg.do_aggregate_and_send(msg)
        aggs_sent = component.state.get("stats.aggs_sent", 0) + num_sent
        component.update({"stats.agg_rpcs": agg_rpcs, "stats.aggs_sent": aggs_sent})

    def make_timers():
        """
        Create one timer for each aggregator config. An aggregator config can
        trigger the aggregation of multiple metrics.
        """
    
        timers = []
        for cfg in config.get("/aggregate"):
            if cfg["agg_class"] == "job":
                interval = cfg["interval"]
                t = RepeatEvent(scheduler, interval, _aggregate_rpc, **cfg)
                timers.append(t)
        log.info("made timers for jobid '%s': %r" % (pargs.jobid, timers))
        return timers

    def show_mcache(msg):
        return jagg.metric_caches

    def subscribe_collectors(__msg):
        # subscribe to collectors
        for rpc_path in collectors_rpc_paths:
            log.info( "subscribing to msgs of job %s at %s" % (pargs.jobid, rpc_path) )
            send_rpc(etcd_client, rpc_path, "subscribe", TARGET="tcp://%s:%d" % (me_addr, recv_port),
                     J=pargs.jobid)

    def unsubscribe_and_quit(__msg):
        for rpc_path in collectors_rpc_paths:
            log.info( "unsubscribing jobid %s from %s" % (pargs.jobid, rpc_path) )
            send_rpc(etcd_client, rpc_path, "unsubscribe", TARGET="tcp://%s:%d" % (me_addr, recv_port))
        os._exit(0)


    # subscribe to collectors
    subscribe_collectors(None)

    component.rpc.register_rpc("agg", aggregate_rpc)
    component.rpc.register_rpc("quit", unsubscribe_and_quit, early_reply=True)
    component.rpc.register_rpc("resubscribe", subscribe_collectors)
    component.rpc.register_rpc("show_mcache", show_mcache)
    component.rpc.register_rpc("resend_state", component.reset_timer)

    make_timers()

    tstart = None
    log.info( "Started msg receiver on %s" % pargs.listen )
    count = 0
    while not main_stopping:
        try:
            s = receiver.recv()
            #log.debug("received msg on PULL port: %r" % s)
            msg = json.loads(s)

            jagg.queue.put(msg)
            if count == 0:
                tstart = time.time()
                count = 0
            count += 1
            component.update({"stats.val_msgs_recvd": count})
            if (pargs.stats and count % 10000 == 0):
                tend = time.time()
                sys.stderr.write("%d msgs in %f seconds, %f msg/s\n" %
                                 (count, tend - tstart, float(count)/(tend - tstart)))
                sys.stderr.flush()
        except zmq.error.Again as e:
            continue
        except Exception as e:
            print "Exception in msg receiver: %r" % e
            jagg.stopping = True
            break

    time.sleep(0.1)
    print "%d messages received" % count
    #pdb.set_trace()
    component.rpc.stop()
    component.rpc.join()
    jagg.stopping = True
    scheduler.stop()
    scheduler.join()
    os._exit(0)


if __name__ == "__main__":
    aggmon_jobagg(sys.argv)
