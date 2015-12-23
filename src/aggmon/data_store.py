#!/usr/bin/python

import argparse
import logging
import os
import sys
import re
import pdb
from pymongo import MongoClient, ASCENDING
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
from agg_mcache import MCache
# Path Fix
sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), "../")))
from metric_store.mongodb_store import *


log = logging.getLogger( __name__ )


class DataStore(threading.Thread):
    def __init__(self, hostname, port, db_name, username="", password="",
                 group="/universe", coll_prefix="gmetric", value_metrics_ttl=180*24*3600):
        self.queue = Queue()
        self.group = group
        self.coll_prefix = coll_prefix
        self.value_metrics_ttl = value_metrics_ttl
        self.store = MongoDBMetricStore(host_name=hostname, port=port, db_name=db_name,
                                        username=username, password=password, group=group)
        if self.store is None:
            raise Exception("Could not connect to Mongo DB")
        self.md_cache = MCache()
        self.v_cache = MCache()
        self.load_md_cache()
        self.stopping = False
        threading.Thread.__init__(self, name="mongo_store")
        self.daemon = True

    def run(self):
        log.info( "[Started DataStore Thread]" )
        self.req_worker()

    def load_md_cache(self):
        log.info("loading md cache ...")
        for d in self.store.find_md( match={"CLUSTER": self.group} ):
            self.md_cache.set(d["HOST"], d["NAME"], d["TYPE"])

    def show_md_cache(self):
        for k, v in self.md_cache.items():
            print("%s: %s" % (k, v))
    
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
    
            try:
                #
                # do the work
                #
                log.debug("mongo_store: val = %r" % val)
                if "SLOPE" in val:
                    # this must be a metadata record coming from gmond/ganglia
                    # is this in cache?
                    _type = self.md_cache.get(val["HOST"], val["NAME"])
                    if _type is not None and _type == val["TYPE"]:
                        log.debug( "skipping MD insert for host=%s, metric=%s" % (val["HOST"], val["NAME"]) )
                        continue
                    val["CLUSTER"] = self.group
                    res = self.store.insert_md(val)
                    log.debug("insert_metadata returned: %r" % res)
                    if "upserted" in res:
                        log.debug( "feeding cache:", val["HOST"], val["NAME"], res["upserted"], val["TYPE"] )
                        self.md_cache.set(val["HOST"], val["NAME"], val["TYPE"])
                    log.debug( "upserted %r" % val )
                else:
                    # is this in cache?
                    _type = self.md_cache.get(val["H"], val["N"])
                    if _type is None:
                        log.error( "WARNING: metadata not found for this value record: %r" % val )
                        #
                        # make a metadata entry
                        #
                        v = val["V"]
                        _type = "string"
                        if isinstance(v, int):
                            _type = "int32"
                        elif isinstance(v, float):
                            _type = "float"
                        md = {"HOST": val["H"], "NAME": val["N"], "TYPE": _type, "CLUSTER": self.group}
                        res = self.store.insert_md(md)
                        log.debug("insert_metadata returned: %r" % res)
                        if "upserted" in res:
                            log.debug( "feeding cache:", md["HOST"], md["NAME"], res["upserted"], md["TYPE"] )
                            self.md_cache.set(md["HOST"], md["NAME"], md["TYPE"])

                    _time = self.v_cache.get(val["H"], val["N"])
                    if _time is not None and _time == val["T"]:
                        log.debug( "skipping V insert for host=%s, metric=%s: duplicate record for time %r" % (val["H"], val["N"], val["T"]) )
                        continue

                    #string|int8|uint8|int16|uint16|int32|uint32|float|double
                    if _type in ("int8", "uint8", "int16", "uint16", "int32", "uint32"):
                        if not isinstance(val["V"], int):
                            val["V"] = int(val["V"])
                    elif _type in ("float", "double"):
                        if not isinstance(val["V"], float):
                            val["V"] = float(val["V"])
                    res = self.store.insert_val(val)
                    log.debug( "inserted %r" % val )
                    self.v_cache.set(val["H"], val["N"], val["T"])
            except Exception as e:
                log.error( "Exception in data_store req worker: %r, %r" % (e, val) )

            self.queue.task_done()


def aggmon_data_store(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument('-g', '--group', default="universe", action="store", help="group/cluster served by this daemon instance")
    ap.add_argument('-C', '--cmd-port', default="tcp://0.0.0.0:5511", action="store", help="RPC command port")
    ap.add_argument('-D', '--dispatcher', default="", action="store", help="agg_control dispatcher RPC command port")
    ap.add_argument('-e', '--expire', default=180, action="store", help="days for expiring value metrics")
    ap.add_argument('-H', '--host', default="localhost:27017", action="store", help="MongoDB host:port")
    ap.add_argument('-d', '--dbname', default="metricdb", action="store", help="database name")
    ap.add_argument('-P', '--prefix', default="gmetric", action="store", help="collections prefix")
    ap.add_argument('-u', '--user', default="", action="store", help="user name")
    ap.add_argument('-p', '--passwd', default="", action="store", help="password")
    ap.add_argument('-l', '--log', default="info", action="store", help="logging: info, debug, ...")
    ap.add_argument('-L', '--listen', default="tcp://0.0.0.0:5550", action="store", help="zmq pull port to listen on")
    ap.add_argument('-M', '--msgbus', default=[], action="append",
                    help="subscription port(s) for message bus. can be used multiple times.")
    ap.add_argument('-s', '--stats', default=False, action="store_true", help="print statistics info")
    ap.add_argument('-v', '--verbose', type=int, default=0, action="store", help="verbosity")
    pargs = ap.parse_args(argv)

    log_level = eval("logging."+pargs.log.upper())
    FMT = "%(asctime)s %(levelname)-5.5s [%(name)s][%(threadName)s] %(message)s"
    logging.basicConfig( stream=sys.stderr, level=log_level, format=FMT )

    # open mongo/toku DB
    mongo_host, mongo_port = pargs.host.split(":")
    mongo_port = int(mongo_port)

    try:
        store = DataStore(mongo_host, mongo_port, pargs.dbname, pargs.user, pargs.passwd,
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
    rpc.register_rpc("quit", unsubscribe_and_quit)
    rpc.register_rpc("resubscribe", subscribe_collectors)

    # subscribe to message bus
    subscribe_collectors(None)

    if len(pargs.dispatcher) > 0:
        me_addr = zmq_own_addr_for_uri(pargs.dispatcher)
        me_listen = "tcp://%s:%d" % (me_addr, recv_port)
        me_rpc = "tcp://%s:%d" % (me_addr, rpc.port)
        state = get_kwds(component="data_store", cmd_port=me_rpc, listen=me_listen, group=pargs.group)
        component = ComponentState(context, pargs.dispatcher, state=state)


    tstart = None
    log.info( "Started msg receiver on %s" % pargs.listen )
    count = 0
    while True:
        try:
            s = receiver.recv()
            #log.info("received msg on PULL port: %r" % s)
            msg = json.loads(s)

            cmd = None
            if "_COMMAND_" in msg:
                log.info("_COMMAND_ received: msg = %r" % msg)
                cmd = msg["_COMMAND_"]

            if cmd is not None:
                if cmd["cmd"] == "quit":
                    log.info( "Stopping mongo_store on 'quit' command.")
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
            count += 1
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
    aggmon_data_store(sys.argv)
