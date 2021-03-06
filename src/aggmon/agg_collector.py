#!/usr/bin/python

#
# Manages subscriptions and matches messages according to them.
#

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
try:
    import ujson as json
except ImportError:
    import json

import zmq
from Queue import Queue, Empty
from agg_component import get_kwds, ComponentState
from msg_tagger import MsgTagger
from agg_rpc import *


log = logging.getLogger( __name__ )
component = None


class AggPubMatch(object):
    """
    Manage subscribers and match messages to the subscribers' match conditions.
    Messages are supposed to be dicts.
    Subscriber match conditions are key-value pairs. The key must match a key
    in the message. If the value matches as well, the subscriber will receive
    the message. If the value string starts with "RE:" then the rest of the string
    is regarded as regular expression match string.
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
    def __init__(self, zmq_context, queue, subs={}, tagger=None):
        self.queue = queue
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
        

class SubscriberQueue(threading.Thread):
    def __init__(self, zmq_context, listen="tcp://0.0.0.0:5555", pre=[]):
        """
        pre: chain of functions that processes the message before putting it into the queue
        """
        self.queue = Queue()
        self.pre = pre
        # Socket to receive messages on
        self.receiver = zmq_context.socket(zmq.PULL)
        self.receiver.setsockopt(zmq.RCVHWM, 40000)
        self.port = zmq_socket_bind_range(self.receiver, listen)
        assert( self.port is not None)
        self.listen = ":".join(listen.split(":")[:2] + [str(self.port)])
        self.stopping = False
        threading.Thread.__init__(self)
        self.daemon = True

    def run(self):
        log.info( "[Started SubCmdThread listening on %s]" % self.listen )
        self.msg_receiver()

    def msg_receiver(self):
        global component
        # Start our clock now
        tstart = None
        
        log.info( "Started msg receiver on %s" % self.listen )
        count = 0
        while not self.stopping:
            try:
                s = self.receiver.recv()
                msg = json.loads(s)
                #log.debug("received: %r" % msg)

                for pre in self.pre:
                    msg = pre(msg)

                self.queue.put(msg)
                if count == 0:
                    tstart = time.time()
                count += 1
                if component is not None:
                    component.update({"stats.msgs_recvd": count})
                if count % 10000 == 0 or "PRINT" in msg:
                #if count % 10 == 0 or "PRINT" in msg:
                    tend = time.time()
                    sys.stdout.write("%d msgs in %f seconds, %f msg/s\n" %
                                     (count, tend - tstart, float(count)/(tend - tstart)))
                    if "PRINT" in msg:
                        sys.stdout.write("Msg is %r\ncount = %d" % (msg, count))
                    sys.stdout.flush()
            except Exception as e:
                log.error("Exception in msg receiver: %r" % e)
                # if something breaks, continue anyway
                #break
        log.info("%d messages received" % count)



def load_state(name):
    try:
        fp = open(name)
        state = pickle.load(fp)
        fp.close()
    except Exception as e:
        print "Exception in AggConfig load '%s': %r" % (name, e)
        return []
    return state

def save_state(name, state):
    """
    'name' is the filename where to store the state
    'state' is a list of objects to be saved
    """
    try:
        fp = open(name, "w")
        pickle.dump(state, fp)
        fp.close()
    except Exception as e:
        print "Exception in AggConfig save '%s': %r" % (name, e)
        return False
    return True


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


def aggmon_collector(argv):
    global component

    ap = argparse.ArgumentParser()
    ap.add_argument('-C', '--cmd-port', default="tcp://127.0.0.1:5556", action="store", help="RPC command port")
    ap.add_argument('-D', '--dispatcher', default="", action="store", help="agg_control dispatcher RPC command port")
    ap.add_argument('-g', '--group', default="universe", action="store", help="group for this message bus. Default: /universe")
    ap.add_argument('-l', '--log', default="info", action="store", help="logging: info, debug, ...")
    ap.add_argument('-L', '--listen', default="tcp://127.0.0.1:5555", action="store", help="zmq pull port to listen on")
    ap.add_argument('-M', '--msgbus', default="", action="store", help="subscription port for other message bus")
    ap.add_argument('-s', '--stats', default=False, action="store_true", help="print statistics info")
    ap.add_argument('-S', '--state-file', default="agg_collector.state", action="store", help="file to store tagger rules and subscriptions")
    ap.add_argument('-v', '--verbose', type=int, default=0, action="store", help="verbosity")
    pargs = ap.parse_args(argv)

    log_level = eval("logging."+pargs.log.upper())
    FMT = "%(asctime)s %(levelname)-5.5s [%(name)s][%(threadName)s] %(message)s"
    logging.basicConfig( stream=sys.stderr, level=log_level, format=FMT )

    state = []
    subs = {}
    tags = {}

    # EF 6.7.16 disabled state loading
    #if len(pargs.state_file) > 0:
    #    state = load_state(pargs.state_file)
    if len(state) >= 2:
        subs = state[0]
        tags = state[1]

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

    def save_subs_tags(msg):
        # EF 6.7.16 disabled state saving
        #save_state(pargs.state_file, [pubsub.subs, tagger.tags])
        pass

    def quit(msg):
        subq.stopping = True
        # raw exit for now
        os._exit(0)

    try:
        context = zmq.Context()
        subq = SubscriberQueue(context, pargs.listen, pre=[spoofed_host, convert_str_int_float])

        tagger = MsgTagger(tags=tags)
        pubsub = AggPubThread(context, subq.queue, subs=subs, tagger=tagger.do_tag)
    
        rpc = RPCThread(context, listen=pargs.cmd_port)
        rpc.start()
    except Exception as e:
        log.error(traceback.format_exc())
        log.error("Failed to initialize something essential. Exiting.")
        os._exit(1)

    rpc.register_rpc("subscribe", pubsub.subscribe, post=save_subs_tags)
    rpc.register_rpc("unsubscribe", pubsub.unsubscribe, post=save_subs_tags)
    rpc.register_rpc("show_subs", pubsub.show_subscriptions)
    rpc.register_rpc("reset_subs", pubsub.reset_subscriptions, post=save_subs_tags)
    rpc.register_rpc("add_tag", tagger.add_tag, post=save_subs_tags)
    rpc.register_rpc("remove_tag", tagger.remove_tag, post=save_subs_tags)
    rpc.register_rpc("reset_tags", tagger.reset_tags, post=save_subs_tags)
    rpc.register_rpc("show_tags", tagger.show_tags)
    rpc.register_rpc("quit", quit, early_reply=True)

    pubsub.start()
    subq.start()

    if len(pargs.dispatcher) > 0:
        me_addr = zmq_own_addr_for_uri(pargs.dispatcher)
        me_listen = "tcp://%s:%d" % (me_addr, subq.port)
        me_rpc = "tcp://%s:%d" % (me_addr, rpc.port)
        state = get_kwds(component="collector", cmd_port=me_rpc, listen=me_listen, group=pargs.group)
        component = ComponentState(context, pargs.dispatcher, state=state)
        rpc.register_rpc("resend_state", component.reset_timer)

    if len(pargs.msgbus) > 0:
        print "subscribing to all msgs from %s" % pargs.msgbus
        msg = {"TARGET": pargs.listen}
        send_rpc(context, pargs.msgbus, "subscribe", **msg)

    while True:
        try:
            subq.join(0.1)
        except Exception as e:
            print "main thread exception: %r" % e
            break

if __name__ == "__main__":
    aggmon_collector(sys.argv)
