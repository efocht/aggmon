#!/usr/bin/python

import argparse
import logging
import sys
import re
import pdb
from pymongo import MongoClient, ASCENDING
import threading
import time
import ujson
import zmq
from Queue import Queue, Empty
from agg_component import get_kwds, ComponentState
from agg_mcache import MCache
from agg_rpc import *
import basic_aggregators as aggs


log = logging.getLogger( __name__ )


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
               of the own group or one on a higher level or the mongo store.
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
        self.send_socket[target].send(msg)
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
        for metric in cmd["metrics"]:
            # TODO: add regexp case
            agg_method = cmd["agg_method"]
            push_target = cmd["push_target"]
            agg_metric = cmd["agg_metric_name"] % locals()
            ttl = None
            if "ttl" in cmd:
                ttl = cmd["ttl"]
            now = int(time.time())

            values = []
            if metric not in self.metric_caches:
                continue
            for host in self.metric_caches[metric]:
                t, v = self.metric_caches[metric][host]
                if ttl is not None and t < now - ttl:
                    continue
                values.append(v)
            agg_value = aggs.aggregate(agg_method, values)
            metric = {"N": agg_metric, "J": self.jobid, "T": now, "V": agg_value}
            log.debug("agg metric = %r, pushing it to %s" % (metric, push_target))
            rc = self.zmq_push.send(push_target, metric)

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
                msg = self.queue.get()
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
                log.debug("job_agg: msg = %r" % msg)
                if "_COMMAND_" in msg:
                    # this must be a command record
                    cmdlist = msg["_COMMAND_"]
                    # execute it, means: do some aggregation
                    if cmdlist["cmd"] == "agg":
                        self.do_aggregate_and_send(cmdlist)

                elif "V" in msg:
                    # this is a value message
                    metric = msg["N"]
                    # add it to the cache
                    if metric not in self.metric_caches:
                        log.debug("job_agg: creating mcache for metric '%s'" % metric)
                        self.metric_caches[metric] = MCache()
                    val = (msg["T"], msg["V"])
                    log.debug("job_agg: inserting %r to cache" % val)
                    self.metric_caches[metric].set(msg["H"], val)

            except Exception as e:
                log.error( "Exception in job_agg req worker: %r, %r" % (e, msg) )

            self.queue.task_done()

def aggmon_jobagg(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument('-C', '--cmd-port', default="tcp://0.0.0.0:5501", action="store", help="RPC command port")
    ap.add_argument('-D', '--dispatcher', default="", action="store", help="agg_control dispatcher RPC command port")
    ap.add_argument('-j', '--jobid', default="", action="store", help="jobid for which this instance does aggregation")
    ap.add_argument('-l', '--log', default="info", action="store", help="logging: info, debug, ...")
    ap.add_argument('-L', '--listen', default="tcp://127.0.0.1:5560", action="store", help="zmq pull port to listen on")
    ap.add_argument('-M', '--msgbus', default=[], action="append",
                    help="subscription port(s) for message bus. can be used multiple times.")
    ap.add_argument('-s', '--stats', default=False, action="store_true", help="print statistics info")
    ap.add_argument('-v', '--verbose', type=int, default=0, action="store", help="verbosity")
    pargs = ap.parse_args(argv)

    log_level = eval("logging."+pargs.log.upper())
    FMT = "%(asctime)s %(levelname)-5.5s [%(name)s][%(threadName)s] %(message)s"
    logging.basicConfig( stream=sys.stderr, level=log_level, format=FMT )

    if len(pargs.jobid) == 0:
        log.error("jobid argument can not be empty!")
        sys.exit(1)

    context = zmq.Context()
    try:
        jagg = JobAggregator(pargs.jobid, context)
    except Exception as e:
        log.error("Failed to create JobAggregator: %r" % e)
        sys.exit(1)
    jagg.start()

    # Socket to receive messages on
    receiver = context.socket(zmq.PULL)
    receiver.setsockopt(zmq.RCVHWM, 40000)
    recv_port = zmq_socket_bind_range(receiver, pargs.listen)
    assert(recv_port is not None)


    def subscribe_collectors(__msg):
        for msgb in pargs.msgbus:
            log.info( "subscribing to msgs of job %s at %s" % (pargs.jobid, msgb) )
            me_addr = zmq_own_addr_for_uri(msgb)
            send_rpc(context, msgb, "subscribe", TARGET="tcp://%s:%d" % (me_addr, recv_port),
                     J=pargs.jobid)


    def unsubscribe_and_quit(__msg):
        # subscribe to message bus
        for msgb in pargs.msgbus:
            log.info( "unsubscribing jobid %s from %s" % (pargs.jobid, msgb) )
            me_addr = zmq_own_addr_for_uri(msgb)
            send_rpc(context, msgb, "unsubscribe", TARGET="tcp://%s:%d" % (me_addr, recv_port))
        os._exit(0)

    rpc = RPCThread(context, listen=pargs.cmd_port)
    rpc.start()
    rpc.register_rpc("quit", unsubscribe_and_quit)
    rpc.register_rpc("resubscribe", subscribe_collectors)

    # subscribe to message bus
    subscribe_collectors(None)

    component = None
    if len(pargs.dispatcher) > 0:
        me_addr = zmq_own_addr_for_uri(pargs.dispatcher)
        me_listen = "tcp://%s:%d" % (me_addr, recv_port)
        me_rpc = "tcp://%s:%d" % (me_addr, rpc.port)
        state = get_kwds(component="job_agg", cmd_port=me_rpc, listen=me_listen, jobid=pargs.jobid)
        component = ComponentState(context, pargs.dispatcher, state=state)
                 

    tstart = None
    log.info( "Started msg receiver on %s" % pargs.listen )
    count = 0
    while True:
        try:
            s = receiver.recv()
            log.debug("received msg on PULL port: %r" % s)
            msg = ujson.loads(s)

            cmd = None
            if "_COMMAND_" in msg:
                cmd = msg["_COMMAND_"]

            if cmd is not None:
                if cmd["cmd"] == "quit":
                    log.info( "Stopping job aggregator for jobid %s on 'quit' command." % pargs.jobid )
                    break
                elif cmd["cmd"] == "resend_state":
                    log.info( "State resend requested." )
                    if component is not None:
                        component.reset_timer()
                    continue

            jagg.queue.put(msg)
            if count == 0 or (cmd is not None and cmd["cmd"] == "reset-stats"):
                tstart = time.time()
                count = 0
            count += 1
            if (pargs.stats and count % 10000 == 0) or \
               (cmd is not None and cmd["cmd"] == "show-stats"):
                tend = time.time()
                sys.stderr.write("%d msgs in %f seconds, %f msg/s\n" %
                                 (count, tend - tstart, float(count)/(tend - tstart)))
                sys.stderr.flush()
        except Exception as e:
            print "Exception in msg receiver: %r" % e
            jagg.stopping = True
            break

    time.sleep(0.1)
    print "%d messages received" % count
    

if __name__ == "__main__":
    aggmon_jobagg(sys.argv)
