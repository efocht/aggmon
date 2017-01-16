import logging
import os
import pdb
import socket
import threading
import time
import traceback
try:
    import ujson as json
except:
    import json


RPC_TIMEOUT = int(os.environ.get("AGG_RPC_TIMEOUT", 10)) # 10 seconds by default
log = logging.getLogger( __name__ )


class RPCNoReplyError(Exception):
    pass


def _rpc_res_path(rpc_cmd_path):
    return "/".join(rpc_cmd_path.split("/")[:-1]) + "/rpc_out"


def send_rpc(etcd_client, rpc_cmd_path, cmd, _RPC_TIMEOUT_=RPC_TIMEOUT, **kwds):
    """
    Send an rpc request to a command server.

    rpc_cmd_path is the path to the directory receiving the rpc commands for a particular service.
    """
    kwds["CMD"] = cmd
    log.debug("sending to %s RPC msg: %r" % (rpc_server, msg))
    req = etcd_client.qput(rpc_cmd_path, kwds)

    req_index = req.createdIndex
    req_key = req.key.split("/")[-1]
    

    result = None
    rpc_res_path = _rpc_res_path(rpc_cmd_path)

    # wait for the result key to appear
    try:
        result = etcd_client.watch(rpc_res_path + "/" + req_key, index=req_index, timeout=_RPC_TIMEOUT_)
    except KeyboardInterrupt:
        return None
    except Exception as e:
        log.error("send_rpc failed with '%r'" % e)

    # decode result and return it
    if "RESULT" in result:
        result = json.loads(result["RESULT"])
        etcd_client.delete(rpc_res_path + "/" + req_key)
    else:
        raise RPCNoReplyError("RPC server at %s did not reply." % rpc_server)
    return result


def own_addr_for_tgt(target):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect((target, 0))
    addr = s.getsockname()[0]
    s.close()
    return addr


def own_addr_for_uri(uri):
    proto, addr, port = uri_split(uri)
    return own_addr_for_tgt(addr)


def uri_split(uri):
    proto, addr, port = uri.split(":")
    addr = addr.lstrip("/")
    return proto, addr, port



class RPCThread(threading.Thread):
    def __init__(self, etcd_client, rpc_cmd_path):
        self.etcd_client = etcd_client
        self.rpc_cmd_path = rpc_cmd_path
        self.rpc_res_path = _rpc_res_path(rpc_cmd_path)
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
        log.info( "[Started RPCThread at %s]" % self.rpc_cmd_path )
        self.rpc_server()

    def rpc_server(self):
        while not self.stopping:
            try:
                key, msg = self.etcd_client.qget(self.rpc_cmd_path) # TODO: implement blocking and timeout
                log.debug( "rpc_server received: %r" % msg )
                if "CMD" in msg:
                    res = "UNKNOWN"
                    cmd = msg["CMD"]
                    del msg["CMD"]
                    if cmd in self.rpcs:
                        func, args, kwargs, post, post_args, early_reply = self.rpcs[cmd]
                        if early_reply is not None:
                            rep_msg = {"RESULT": early_reply}
                            log.debug( "sending RPC reply msg: %r" % rep_msg )
                            self.etcd_client.set(self.rpc_res_path + "/" + key, rep_msg)
                            
                        res = func(msg, *args, **kwargs)

                        # TODO: Error handling. Return ERROR keyword and proper message.

                        ###
                        ### send REPLY
                        ###
                        if early_reply is None:
                            rep_msg = {"RESULT": res}
                            log.debug( "sending RPC reply msg: %r" % rep_msg )
                            self.etcd_client.set(self.rpc_res_path + "/" + key, rep_msg)
    
                        # post handling
                        if post is not None:
                            log.debug( "RPC post()=%r args=%r" % (post, post_args) )
                            post(msg, *post_args)
                    else:
                        rep_msg = {"RESULT": "unknown command: '%s'" % cmd}
                        log.debug( "sending RPC reply msg: %r" % rep_msg )
                        self.etcd_client.set(self.rpc_res_path + "/" + key, rep_msg)

            except Exception as e:
                log.error(traceback.format_exc())
                log.error( "Exception in RPC server: %r" % e )
                log.error( "RPC server: cmd=%s msg=%r" % (cmd, msg) )
                break

    def stop(self):
        self.stopping = True
