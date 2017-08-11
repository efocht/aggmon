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
from hierarchy_helpers import hierarchy_from_url
from agg_rpc import *
from agg_config import Config, DEFAULT_CONFIG_DIR
# Path Fix
sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), "../")))
from metric_store.mongodb_store import MongoDBMetricStore
from metric_store.influxdb_store import InfluxDBMetricStore


log = logging.getLogger( __name__ )
msgs_count = 0


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
                store = MongoDBMetricStore(hostname=hostname, port=port, db_name=db_name,
                                           username=username, password=password, group=group)
            elif backend == "influxdb":
                store = InfluxDBMetricStore(hostname=hostname, port=port, db_name=db_name,
                                            username=username, password=password, group=group)
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
    global msgs_count

    ap = argparse.ArgumentParser()
    ap.add_argument('-H', '--hierarchy-url', default="", action="store",
                    help="position in hierarchy for this component, eg. group:/universe")
    ap.add_argument('-L', '--listen', default="tcp://127.0.0.1:5555",
                    action="store", help="zmq pull port to listen on")
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

    if len(pargs.hierarchy_url) == 0:
        log.error("No hierarchy URL provided for this component. Use the -H option!")
        os._exit(1)
    hierarchy, component_id, hierarchy_path = hierarchy_from_url(pargs.hierarchy_url)
    if hierarchy != "group":
        log.error("Hierarchy '%(hierarchy)s' is currently not supported for data store." % locals())
        os._exit(1)

    etcd_client = EtcdClient()
    config = Config(etcd_client)
    comp = ComponentState(etcd_client, "data_store", pargs.hierarchy_url)

    pargs.backend = pargs.backend.split(",")
    if pargs.port:
        pargs.port = pargs.port.split(",")      # TODO: get rid of this and move it into (shared) config
    else:
        pargs.port = [None, None]

    pdb.set_trace()
    # open DB
    try:
        store = DataStore(pargs.backend, pargs.host, pargs.port, pargs.dbname,
                          pargs.user, pargs.passwd, hierarchy_path,
                          coll_prefix=pargs.prefix, value_metrics_ttl=pargs.expire*24*3600)
    except Exception as e:
        log.error("Failed to create DataStore: %r" % e)
        os._exit(1)
    store.start()

    context = zmq.Context()

    # Socket to receive messages on
    receiver = context.socket(zmq.PULL)
    receiver.setsockopt(zmq.RCVHWM, 40000)
    recv_port = zmq_socket_bind_range(receiver, pargs.listen)
    assert( recv_port is not None)

    me_addr = zmq_own_addr_for_uri(pargs.dispatcher)
    me_listen = "tcp://%s:%d" % (me_addr, recv_port)
    state = get_kwds(listen=me_listen)
    comp.update_state_cache(state)

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

    def reset_stats(__msg):
        global msgs_count
        msgs_count = 0

    comp.start()
    comp.rpc.register_rpc("quit", unsubscribe_and_quit, early_reply=True)
    comp.rpc.register_rpc("resubscribe", subscribe_collectors)
    comp.rpc.register_rpc("resend_state", comp.reset_timer)
    comp.rpc.register_rpc("reset_stats", reset_stats)

    # subscribe to message bus
    subscribe_collectors(None)

    tstart = None
    log.info( "Started msg receiver on %s" % pargs.listen )
    msgs_count = 0
    while True:
        try:
            s = receiver.recv()
            log.debug("received msg on PULL port: %r" % s)
            msg = json.loads(s)

            store.queue.put(msg)
            if count == 0:
                tstart = time.time()
                msgs_count = 0
            msgs_count += 1
            comp.update_state_cache({"stats.msgs_recvd": count})
            if (pargs.stats and count % 100000 == 0):
                tend = time.time()
                sys.stdout.write("%d msgs in %f seconds, %f msg/s\n" %
                                 (msgs_count, tend - tstart,
                                  float(msgs_count)/(tend - tstart)))
                sys.stdout.flush()
        except Exception as e:
            print "Exception in msg receiver: %r" % e
            break
    log.info("THE END")
    os._exit(1)


if __name__ == "__main__":
    aggmon_data_store(sys.argv[1:])
