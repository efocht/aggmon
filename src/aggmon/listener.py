import logging
import sys
import threading
import time
import traceback
import zmq
try:
    import ujson as json
except ImportError:
    import json
from agg_helpers import *


log = logging.getLogger( __name__ )


class Listener(threading.Thread):
    def __init__(self, zmq_context, listen="tcp://0.0.0.0:5555",
                 queue=None, component=None, pre=[]):
        """
        ZMQ Listener: thread that receives messages from a ZMQ listen port and pushes them
        to the queue of some next stage, which lives in a separate thread.
        Arguments:
        zmq_context: ZeroMQ context, usually opened outside and common to a component's stages.
        listen: string with ZMQ URL. The port can be a range of ports, out of which
                one will be chosen.
        queue: the queue of the next stage. The Listener will put() messages to that queue.
        pre: chain of functions that processes the message before putting it into the queue.
        """
        self.queue = queue
        self.comp = component
        self.count = 0
        self.pre = pre
        # Socket to receive messages on
        self.receiver = zmq_context.socket(zmq.PULL)
        self.receiver.setsockopt(zmq.RCVHWM, 40000)
        self.receiver.setsockopt(zmq.RCVTIMEO, 1000)
        self.receiver.setsockopt(zmq.LINGER, 0)

        self.port = zmq_socket_bind_range(self.receiver, listen)
        assert( self.port is not None)

        me_addr = own_addr_for_tgt("8.8.8.8")
        self.listen = "tcp://%s:%d" % (me_addr, self.port)

        self.stopping = False
        threading.Thread.__init__(self)
        self.daemon = True

    def run(self):
        log.info( "[Started Listener on %s]" % self.listen )
        self.msg_receiver()

    def msg_receiver(self):
        # Start our clock now
        tstart = None
        
        log.info( "Started msg receiver on %s" % self.listen )
        self.count = 0
        while not self.stopping:
            try:
                s = self.receiver.recv()
                msg = json.loads(s)
                #log.debug("received: %r" % msg)

                for pre in self.pre:
                    msg = pre(msg)

                self.queue.put(msg)
                if self.count == 0:
                    tstart = time.time()
                count += 1
                if self.comp is not None:
                    self.comp.update_state_cache({"stats.msgs_recvd": self.count})
            except zmq.error.Again as e:
                continue
            except Exception as e:
                log.error("Exception in msg receiver: %r" % e)
                # if something breaks, continue anyway
                #break
        log.info("%d messages received" % self.count)


