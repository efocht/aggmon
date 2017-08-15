#!/usr/bin/python

#
# Manages subscriptions and matches messages according to them.
#

import atexit
import argparse
import logging
import os
try:
    import pdb
except:
    pass
import pickle
import re
import socket
import sys
import threading
import time
import traceback
import zmq
try:
    import ujson as json
except ImportError:
    import json

from Queue import Queue, Empty
from agg_helpers import *
from agg_component import get_kwds, ComponentState
from agg_config import Config, DEFAULT_CONFIG_DIR
from agg_rpc import *
from msg_tagger import MsgTagger
from listener import Listener
from etcd import EtcdWatchTimedOut


log = logging.getLogger( __name__ )
# The component object is global
comp = None

#
# Helper functions
#
def convert_str_int_float(msg):
    # convert string values to int or float
    if "V" in msg:
        val = msg["V"]
        if isinstance(val, basestring):
            if val.isdigit():
                val = int(val)
                msg["V"] = val
            else:
                try:
                    val = float(val)
                    msg["V"] = val
                except ValueError:
                    pass
    return msg


class AggPubMatch(object):
    """
    Manage subscribers and match messages to the subscribers' match conditions.
    Messages are supposed to be dicts.
    Subscriber match conditions are key-value pairs. The key must match a key
    in the message. If the value matches as well, the subscriber will receive
    the message. If the value string starts with "RE:" then the rest of the string
    is regarded as regular expression match string.
    Subscribing with an empty dict means: match all messages.
    """
    def __init__(self, zmq_context, subs={}):
        self.zmq_context = zmq_context
        self.send_socket = {}
        self.socket_conn = {}
        self.subs = subs
        if subs != {}:
            for target in subs.keys():
                sock = self.zmq_context.socket(zmq.PUSH)
                self.send_socket[target] = sock

    @staticmethod
    def _eq_msg_sub(msg, sub):
        # compare msg with subscription dict
        equal = True
        for k, v in msg.items():
            if k not in sub:
                equal = False
                break
            if sub[k]["s"] != v:
                equal = False
                break
        if equal and len(set(sub.keys()) - set(msg.keys())) > 0:
            equal = False
        return equal

    def publish(self, msg):
        jmsg = json.dumps(msg)
        targets = self.match(msg)
        log.debug("publishing msg %r to targets %r" % (msg, targets))
        for target in targets:
            sock = self.send_socket[target]
            if not target in self.socket_conn:
                log.info( "connecting to %r" % target )
                sock.connect(target)
                self.socket_conn[target] = 1
            try:
                sock.send_string(jmsg, flags=zmq.NOBLOCK)
            except:
                pass

    def show_subscriptions(self, msg, *args, **kwds):
        return self.subs

    def reset_subscriptions(self, msg):
        for target in self.subs.keys():
            self.unsubscribe({"TARGET": target})

    def subscribe(self, msg, *args, **kwds):
        if "TARGET" not in msg:
            raise Exception("No TARGET in subscription command message!")
        target = msg["TARGET"]
        del msg["TARGET"]
        for k, v in msg.items():
            if isinstance(v, basestring):
                if v.startswith("RE:"):
                    matchre = "RE:".join(v.split("RE:")[1:])
                    comp = re.compile(matchre)
                    msg[k] = {"s": v, "c": comp}
                else:
                    msg[k] = {"s": v}
        if target in self.subs:
            # check if subscription already here!
            for s in self.subs[target]:
                if s == msg:
                    log.info("subscription already existing! skipping subscribe.")
                    return target
            self.subs[target].append(msg)
        else:
            self.subs[target] = [msg]
            sock = self.zmq_context.socket(zmq.PUSH)
            sock.setsockopt(zmq.SNDHWM, 100000)
            self.send_socket[target] = sock
        log.info( "subscribed target '%s' with topic(s) %r" % (target, msg) )
        return target

    def unsubscribe(self, msg, *args, **kwds):
        if "TARGET" not in msg:
            raise Exception("No TARGET in unsubscribe command message!")
        target = msg["TARGET"]
        del msg["TARGET"]
        if target in self.subs:
            if len(msg) > 0:
                for i in xrange(len(self.subs[target])):
                    sub = self.subs[target][i]
                    if self._eq_msg_sub(msg, sub):
                        del self.subs[target][i]
            if len(msg) == 0 or (len(msg) > 0 and len(self.subs[target]) == 0):
                del self.subs[target]
                if target in self.send_socket:
                    self.send_socket[target].close()
                    del self.send_socket[target]
                if target in self.socket_conn:
                    del self.socket_conn[target]

    def match(self, msg):
        """
        Return targets that match this message in an array.
        """
        targets = []
        for target in self.subs.keys():
            for match_dict in self.subs[target]:
                # matches is True by default for the case that someone subscribed to all messages
                # which means: no match keys associated with a target.
                matches = True
                for mk, mv in match_dict.items():
                    if mk not in msg:
                        matches = False
                        break
                    v = msg[mk]
                    if "c" in mv:
                        if mv["c"].match(v) is None:
                            matches = False
                            break
                    elif mv["s"] != v:
                        matches = False
                        break
                if matches:
                    targets.append(target)
        return targets


class AggPubThread(threading.Thread, AggPubMatch):
    def __init__(self, zmq_context, subs={}, tagger=None):
        self.queue = Queue()
        self.tagger = tagger
        self.stopping = False
        threading.Thread.__init__(self)
        self.daemon = True
        AggPubMatch.__init__(self, zmq_context, subs=subs)

    def run(self):
        global component
        log.info("[Started AggPubThread]")
        count = 0
        while not self.stopping:
            try:
                msg = self.queue.get()
                #msg = json.loads(s)
                self.queue.task_done()
                #msg = self.queue.popleft()
            except (KeyboardInterrupt, SystemExit) as e:
                self.stopping = True
                log.info( "Interrupt or exit: ", e )
                continue
            except Empty:
                time.sleep(0.05)
                continue
            except IndexError:
                time.sleep(0.05)
                continue
            except Exception as e:
                log.error( "s=%s" % s )
                log.error( "type(s) = ", type(s) )
                log.error( "Exception: %r" % e )
                raise e
            try:
                if self.tagger is not None:
                    msg = self.tagger(msg)
                #log.debug( "publishing msg: %r" % msg )
                self.publish(msg)
                if count == 0:
                    tstart = time.time()
                    bstart = tstart
                count += 1
                if component is not None:
                    component.update({"stats.msgs_published": count})
                if count % 10000 == 0:
                    tend = time.time()
                    sys.stdout.write("published %d msgs in %f seconds, %f msg/s\n" %
                                     (count, tend - tstart, float(count)/(tend - tstart)))
                    sys.stdout.flush()
            except Exception as e:
                log.error( "Exception in AggPubThread: %r" % e )
                continue


def test_pub():
    """
    Test for publisher matcher mechanism
    """
    import random

    a = AggPubSub()
    a.subscribe("a_to_d", **{"word": "RE:[a-d].*"})
    a.subscribe("e_to_q", **{"word": "RE:[e-q].*"})
    a.subscribe("all")
    a.subscribe("none", **{"nokey": 123})

    tstart = time.time()
    res = {}
    for x in xrange(100000):
        s = chr(random.randrange(97, 122)) + "xyzabc"
        msg = {"dummy": 1234, "word": s}
        for m in a.match(msg):
            if m in res:
                res[m] += 1
            else:
                res[m] = 1
    tend = time.time()

    print "Matched in %f seconds:" % (tend - tstart)
    for k, v in res.items():
        print "%-10s %d" % (k, v)


JOB_HPATH = "/config/hierarchy/job"

def aggmon_collector(argv):
    global comp

    ap = argparse.ArgumentParser()
    ap.add_argument('-H', '--hierarchy-url', default="", action="store",
                    help="position in hierarchy for this component, eg. group:/universe")
    ap.add_argument('-l', '--log', default="info", action="store",
                    help="logging: info, debug, ...")
    ap.add_argument('-L', '--listen', default="tcp://127.0.0.1:5555",
                    action="store", help="zmq pull port to listen on")
    ap.add_argument('-M', '--msgbus', default="", action="store",
                    help="subscription port for other message bus, eg. for testing")
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
    
    def spoofed_host(msg):
        # treat spoofed hosts already here
        if "H" in msg and ":" in msg["H"]:
            msg["H"] = msg["H"].split(":")[1]
        elif "HOST" in msg and ":" in msg["HOST"]:
            msg["HOST"] = msg["HOST"].split(":")[1]
        ##
        ## For debugging duplicates...
        ##
        #if "N" in msg and msg["N"] == "cpu_user":
        #    log.info("cpu_user val: %r" % msg)
        return msg

    etcd_client = EtcdClient()
    config = Config(etcd_client)
    comp = ComponentState(etcd_client, "collector", pargs.hierarchy_url)

    state = {}
    saved_subs = comp.get_data("/subs")
    if saved_subs is None:
        saved_subs = {}
    saved_tags = comp.get_data("/tags")
    if saved_tags is None:
        saved_tags = {}

    try:
        context = zmq.Context()
        tagger = MsgTagger(tags=saved_tags)
        pubsub = AggPubThread(context, subs=saved_subs, tagger=tagger.do_tag)
        listener = Listener(context, pargs.listen, queue=pubsub.queue, component=comp,
                            pre=[spoofed_host, convert_str_int_float])
    except Exception as e:
        log.error(traceback.format_exc())
        log.error("Failed to initialize something essential. Exiting.")
        os._exit(1)

    state = get_kwds(listen=listener.listen)
    comp.update_state_cache(state)

    pubsub.start()
    listener.start()
    comp.start()
    atexit.register(join_threads, listener)

    #
    # RPC functions and helpers
    #
    def quit(msg):
        log.info("'quit' rpc received.")
        listener.stopping = True
        # raw exit for now
        os._exit(0)

    def save_subs_data(msg):
        comp.set_data("/subs", pubsub.subs)

    def save_tags_data(msg):
        comp.set_data("/tags", tagger.tags)

    comp.rpc.register_rpc("subscribe", pubsub.subscribe, post=save_subs_data)
    comp.rpc.register_rpc("unsubscribe", pubsub.unsubscribe, post=save_subs_data)
    comp.rpc.register_rpc("show_subs", pubsub.show_subscriptions)
    comp.rpc.register_rpc("reset_subs", pubsub.reset_subscriptions, post=save_subs_data)
    comp.rpc.register_rpc("add_tag", tagger.add_tag, post=save_tags_data)
    comp.rpc.register_rpc("remove_tag", tagger.remove_tag, post=save_tags_data)
    comp.rpc.register_rpc("reset_tags", tagger.reset_tags, post=save_tags_data)
    comp.rpc.register_rpc("show_tags", tagger.show_tags)
    comp.rpc.register_rpc("quit", quit, early_reply=True)
    comp.rpc.register_rpc("resend_state", comp.reset_timer)

    if len(pargs.msgbus) > 0:
        print "subscribing to all msgs from %s" % pargs.msgbus
        msg = {"TARGET": pargs.listen}
        send_rpc(context, pargs.msgbus, "subscribe", **msg)

    ########################################################
    # main collector configuration/hierarchy update logic
    # ------------------
    # - gather jobs hierarchy info
    # - compare with tagger config and fixup
    # - subscription removal for useless per_job components
    ########################################################

    nsleep = 10
    run = True
    while run:
        # jobs configured in hierachy
        jobs_config = config.get("/hierarchy/job")
        # jobs the tagger currently cares about
        jobs_tagger = [x[2:] for x in tagger.tags.keys() if x.startswith("J:")]
        # 
        # remove tags for closed jobs
        #
        for jobid in set(jobs_tagger) - set(jobs_config):
            tagger.remove_tag(get_kwds(TAG_KEY="J", TAG_VALUE=jobid))
            #
            # unsubscribe subscribers with this job ID
            #
            targets = pubsub.match({"J": jobid})
            for target in targets:
                pubsub.unsubscribe({"TARGET": target, "J": jobid})
        #
        # sleep a while
        #
        sleep = nsleep
        while sleep > 0:
            sleep -= 1
            time.sleep(1)


def join_threads(listener):
    try:
        listener.join(0.1)
    except Exception as e:
        log.error("main thread exception: %r" % e)
    log.debug("leaving...")


if __name__ == "__main__":
    aggmon_collector(sys.argv[1:])
