#!/usr/bin/python

import argparse
import logging
import os
import sys
import re
import pdb
import threading
import time
try:
    import ujson as json
except ImportError:
    import json
import zmq
from Queue import Queue, Empty
from agg_component import get_kwds, ComponentState
from agg_rpc import *
from config import Config, DEFAULT_CONFIG_DIR
# Path Fix
sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), "../")))
from metric_store.mongodb_store import MongoDBMetricStore
from metric_store.influxdb_store import InfluxDBMetricStore


log = logging.getLogger( __name__ )
component = None


class DataStore(threading.Thread):
    def __init__(self, backends, hostname, ports, db_name, username="", password="",
                 group="/universe", coll_prefix="gmetric", value_metrics_ttl=180*24*3600):
        self.backends = backends
        self.queue = Queue()
        self.group = group
        self.coll_prefix = coll_prefix
        self.value_metrics_ttl = value_metrics_ttl
        self.store = []
        for i in xrange(len(self.backends)):
            backend = self.backends[i]
            port = ports[i]
            # TODO: add backend selection to config file
            log.debug("create %s store: %s:%s, %s" % (backend, hostname, str(port), db_name))
            if backend == "mongodb":
                store = MongoDBMetricStore(hostname=hostname, port=port, db_name=db_name, username=username, password=password, group=group)
            elif backend == "influxdb":
                store = InfluxDBMetricStore(hostname=hostname, port=port, db_name=db_name, username=username, password=password, group=group)
            if store is None:
                raise Exception("Could not connect to backend %s" % backend)
            self.store.append(store)
        self.stopping = False
        threading.Thread.__init__(self, name="data_store")
        self.daemon = True

    def run(self):
        log.info( "[Started DataStore Thread]" )
        self.req_worker()

    def req_worker(self):
        while not self.stopping:
            try:
                val = self.queue.get()
            except (KeyboardInterrupt, SystemExit) as e:
                log.warning("Interrupt in thread? %r" % e)
                continue
            except Empty:
                time.sleep(0.05)
                continue
    
            log.debug("data_store insert metric '%r'" % val)
            try:
                for store in self.store:
                    store.insert(val)
            except Exception as e:
                log.error( "Exception in data_store while insert in %r: %r" % (str(store.__class__.__name__), e) )
            self.queue.task_done()


def aggmon_data_store(argv):
    global component

    ap = argparse.ArgumentParser()
    ap.add_argument('-H', '--hierarchy-url', default="", action="store",
                    help="position in hierarchy for this component, eg. group:/universe")
    ap.add_argument('-c', '--config', default=DEFAULT_CONFIG_DIR, action="store", help="configuration directory")
    ap.add_argument('-e', '--expire', default=180, action="store", help="days for expiring value metrics")
    ap.add_argument('-b', '--backend', default="mongodb", action="store", help="database backend(s), comma separated. Default is 'mongodb'.")
    ap.add_argument('-N', '--host', default="localhost", action="store", help="data store host")
    ap.add_argument('-n', '--port', default=None, action="store", help="data store port")
    ap.add_argument('-d', '--dbname', default="metricdb", action="store", help="database name")
    ap.add_argument('-P', '--prefix', default="gmetric", action="store", help="collections prefix")
    ap.add_argument('-u', '--user', default="", action="store", help="user name")
    ap.add_argument('-p', '--passwd', default="", action="store", help="password")
    ap.add_argument('-l', '--log', default="info", action="store", help="logging: info, debug, ...")
    ap.add_argument('-s', '--stats', default=False, action="store_true", help="print statistics info")
    ap.add_argument('-v', '--verbose', type=int, default=0, action="store", help="verbosity")
    pargs = ap.parse_args(argv)

    log_level = eval("logging."+pargs.log.upper())
    FMT = "%(asctime)s %(levelname)-5.5s [%(name)s][%(threadName)s] %(message)s"
    logging.basicConfig( stream=sys.stderr, level=log_level, format=FMT )

    config = Config(config_dir=pargs.config)

    pargs.backend = pargs.backend.split(",")
    if pargs.port:
        pargs.port = pargs.port.split(",")      # TODO: get rid of this and move it into (shared) config
    else:
        pargs.port = [None, None]
    # open DB
    try:
        store = DataStore(pargs.backend, pargs.host, pargs.port, pargs.dbname, pargs.user, pargs.passwd,
                           pargs.group, coll_prefix=pargs.prefix, value_metrics_ttl=pargs.expire*24*3600)
    except Exception as e:
        log.error("Failed to create DataStore: %r" % e)
        sys.exit(1)
    store.start()

    context = zmq.Context()

    # Socket to receive messages on
    receiver = context.socket(zmq.PULL)
    receiver.setsockopt(zmq.RCVHWM, 40000)
    recv_port = zmq_socket_bind_range(receiver, pargs.listen)
    assert( recv_port is not None)


    def subscribe_collectors(__msg):
        for msgb in pargs.msgbus:
            log.info( "subscribing to all msgs at '%s'" % msgb )
            me_addr = zmq_own_addr_for_uri(msgb)
            send_rpc(context, msgb, "subscribe", TARGET="tcp://%s:%d" % (me_addr, recv_port))

    def unsubscribe_and_quit(__msg):
        for msgb in pargs.msgbus:
            log.info( "unsubscribing from '%s'" % msgb )
            me_addr = zmq_own_addr_for_uri(msgb)
            send_rpc(context, msgb, "unsubscribe", TARGET="tcp://%s:%d" % (me_addr, recv_port))
        os._exit(0)


    rpc = RPCThread(context, listen=pargs.cmd_port)
    rpc.start()
    rpc.register_rpc("quit", unsubscribe_and_quit, early_reply=True)
    rpc.register_rpc("resubscribe", subscribe_collectors)

    if len(pargs.dispatcher) > 0:
        me_addr = zmq_own_addr_for_uri(pargs.dispatcher)
        me_listen = "tcp://%s:%d" % (me_addr, recv_port)
        me_rpc = "tcp://%s:%d" % (me_addr, rpc.port)
        state = get_kwds(component="data_store", cmd_port=me_rpc, listen=me_listen, group=pargs.group)
        component = ComponentState(context, pargs.dispatcher, state=state)
        rpc.register_rpc("resend_state", component.reset_timer)

    # subscribe to message bus
    subscribe_collectors(None)

    tstart = None
    log.info( "Started msg receiver on %s" % pargs.listen )
    count = 0
    while True:
        try:
            s = receiver.recv()
            log.debug("received msg on PULL port: %r" % s)
            msg = json.loads(s)

            cmd = None
            if "_COMMAND_" in msg:
                log.info("_COMMAND_ received: msg = %r" % msg)
                cmd = msg["_COMMAND_"]

            if cmd is not None:
                if cmd["cmd"] == "quit":
                    log.info( "Stopping data_store on 'quit' command.")
                    # raw exit!!!
                    os._exit(0)
                    break
                elif cmd["cmd"] == "resend_state":
                    log.info( "State resend requested." )
                    if component is not None:
                        component.reset_timer()
                    continue
            
            store.queue.put(msg)
            if count == 0 or (cmd is not None and cmd["cmd"] == "reset-stats"):
                tstart = time.time()
                count = 0
            count += 1
            if component is not None:
                component.update({"stats.msgs_recvd": count})
            if (pargs.stats and count % 10000 == 0) or \
               (cmd is not None and cmd["cmd"] == "show-stats"):
                tend = time.time()
                sys.stdout.write("%d msgs in %f seconds, %f msg/s\n" %
                                 (count, tend - tstart, float(count)/(tend - tstart)))
                sys.stdout.flush()
        except Exception as e:
            print "Exception in msg receiver: %r" % e
            break
    log.info("THE END")


if __name__ == "__main__":
    aggmon_data_store(sys.argv[1:])
