import logging
import os
import pdb
import socket
import threading
import traceback
try:
    import ujson as json
except:
    import json
import zmq


RPC_TIMEOUT = int(os.environ.get("AGG_RPC_TIMEOUT", 5000)) # 5 seconds by default
log = logging.getLogger( __name__ )


def send_rpc(zmq_context, rpc_server, cmd, **kwds):
    """
    Send an rpc request to a command server.
    """
    socket = zmq_context.socket(zmq.REQ)
    poller = zmq.Poller()
    #log.debug("connecting to %s" % rpc_server)
    socket.connect(rpc_server)
    poller.register(socket)
    kwds["CMD"] = cmd
    msg = json.dumps(kwds)
    log.debug("sending to %s RPC msg: %r" % (rpc_server, msg))
    socket.send(msg, flags=zmq.NOBLOCK)
    try:
        events = poller.poll( timeout=RPC_TIMEOUT )
    except KeyboardInterrupt:
        return None
    if len( events ) != 0:
        for sock, _event in events:
            reply = sock.recv_json()
            log.debug("received result msg: %r" % reply)
            if "RESULT" in reply:
                return reply["RESULT"]
    #log.debug("disconnecting from %s" % rpc_server)
    socket.disconnect(rpc_server)
    socket.close()


def zmq_socket_bind_range(sock, listen):
    """
    This accepts port ranges as argument, for example "tcp://*:6100-6200" or
    even concatenated ranges like "tcp://0.0.0.0:6100-6200,7100-7200".

    Returns the port to which the socket was bound or None in case of error.
    """
    proto, addr, ports = listen.split(":")
    port = None
    if ports.isdigit():
        # this is just a port number, do simple bind
        try:
            sock.bind(listen)
            port = int(ports)
        except Exception as e:
            log.error("zmq_socket_bind_range failed: %r" % e)
    elif "," in ports or "-" in ports:
        for prange in ports.split(","):
            if "-" in prange:
                # this should better be a range of ports
                p_min, p_max = prange.split("-")
                p_min = int(p_min)
                p_max = int(p_max)
                try:
                    port = sock.bind_to_random_port(proto + ":" + addr, min_port=p_min, max_port=p_max, max_tries=100)
                except Exception as e:
                    log.error("zmq_socket_bind_range failed: %r" % e)
                else:
                    break
            elif prange.isdigit():
                try:
                    sock.bind(proto + ":" + addr + ":" + prange)
                    port = int(prange)
                except Exception as e:
                    pass
                else:
                    break
    return port


def zmq_own_addr_for_tgt(target):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect((target, 0))
    addr = s.getsockname()[0]
    s.close()
    return addr


def zmq_own_addr_for_uri(uri):
    proto, addr, port = zmq_uri_split(uri)
    return zmq_own_addr_for_tgt(addr)


def zmq_uri_split(uri):
    proto, addr, port = uri.split(":")
    addr = addr.lstrip("/")
    return proto, addr, port





class RPCThread(threading.Thread):
    def __init__(self, zmq_context, listen="tcp://0.0.0.0:5556"):
        self.responder = zmq_context.socket(zmq.REP)
        self.port = zmq_socket_bind_range(self.responder, listen)
        assert( self.port is not None)
        self.listen = ":".join(listen.split(":")[:2] + [str(self.port)])
        self.stopping = False
        threading.Thread.__init__(self)
        self.daemon = True
        self.rpcs = {}

    # TODO: integrate rpcs
    def register_rpc(self, cmd, function, args=[], kwargs={}, post=None, post_args=[], early_reply=None):
        """
        A rpc function will be called with the arguments: cmd, msg, *args, **kwargs
        """
        self.rpcs[cmd] = (function, args, kwargs, post, post_args, early_reply)

    def run(self):
        log.info( "[Started RPCThread listening on %s]" % self.listen )
        self.rpc_server()

    def rpc_server(self):
        while not self.stopping:
            try:
                s = self.responder.recv()
                msg = json.loads(s)
                log.debug( "rpc_server received: %r" % msg )
                if "CMD" in msg:
                    res = "UNKNOWN"
                    cmd = msg["CMD"]
                    del msg["CMD"]
                    if cmd in self.rpcs:
                        func, args, kwargs, post, post_args, early_reply = self.rpcs[cmd]
                        if early_reply is not None:
                            rep_msg = json.dumps({"RESULT": early_reply})
                            log.debug( "sending RPC reply msg: %r" % rep_msg )
                            self.responder.send(rep_msg)
                            
                        res = func(msg, *args, **kwargs)

                        # TODO: Error handling. Return ERROR keyword and proper message.

                        ###
                        ### send REPLY
                        ###
                        if early_reply is None:
                            rep_msg = json.dumps({"RESULT": res})
                            log.debug( "sending RPC reply msg: %r" % rep_msg )
                            self.responder.send(rep_msg)
    
                        # post handling
                        if post is not None:
                            log.debug( "RPC post()=%r args=%r" % (post, post_args) )
                            post(msg, *post_args)
                    else:
                        rep_msg = json.dumps({"RESULT": "unknown command: '%s'" % cmd})
                        log.debug( "sending RPC reply msg: %r" % rep_msg )
                        self.responder.send(rep_msg)

            except Exception as e:
                log.error(traceback.format_exc())
                log.error( "Exception in RPC server: %r" % e )
                log.error( "RPC server: cmd=%s msg=%r" % (cmd, msg) )
                break

