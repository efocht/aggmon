from etcd_client import *
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

def _basename(a):
    return a.split("/")[-1]


def send_rpc(etcd_client, rpc_cmd_path, cmd, _RPC_TIMEOUT_=RPC_TIMEOUT, **kwds):
    """
    Send an rpc request to a command server.

    rpc_cmd_path is the path to the directory receiving the rpc commands for a particular service.
    """
    kwds["CMD"] = cmd
    log.debug("sending to %s RPC msg: %r" % (rpc_cmd_path, kwds))
    req = etcd_client.qput(rpc_cmd_path, kwds)

    req_index = req.createdIndex
    req_key = req.key.split("/")[-1]
    

    result = None
    rpc_res_path = _rpc_res_path(rpc_cmd_path)

    # wait for the result key to appear
    try:
        result = etcd_client.watch(rpc_res_path + "/" + req_key, index=req_index, timeout=_RPC_TIMEOUT_).value
        result = json.loads(result)
        log.debug("Received result: %r" % result)
    except KeyboardInterrupt:
        return None
    except Exception as e:
        log.error("send_rpc failed with '%r'" % e)

    # decode result and return it
    if "RESULT" in result:
        result = result["RESULT"]
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
                key, msg = self.etcd_client.qget(self.rpc_cmd_path, wait=True, timeout=1)
                key = _basename(key)
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

            except EtcdTimeout:
                continue
            except EtcdQueueEmpty:
                continue
            except EtcdConnectionFailed:
                continue
            except Exception as e:
                log.error(traceback.format_exc())
                log.error( "Exception in RPC server: %r" % e )
                #log.error( "RPC server: key=%s msg=%r" % (key, msg) )
                break

    def stop(self):
        self.stopping = True

#
# a little test
# 
#
if __name__ == "__main__":
    import sys
    log_level = logging.DEBUG
    FMT = "%(asctime)s %(levelname)-5.5s [%(name)s][%(threadName)s] %(message)s"
    logging.basicConfig( stream=sys.stderr, level=log_level, format=FMT )

    #
    # tune logging levels of some of the called components
    #
    log_levels = {
        "etcd.client": logging.CRITICAL,
        "urllib3.connectionpool": logging.WARNING,
    }
    for l_name, l_lev in log_levels.items():
        l = logging.getLogger(l_name)
        l.setLevel(l_lev)

    etcd_client = EtcdClient()
    etcd_client.delete("/RPCTEST", recursive=True)
    etcd_client.write("/RPCTEST/rpc", None, dir=True)
    etcd_client.write("/RPCTEST/rpc_out", None, dir=True)
    rpcthr = RPCThread(etcd_client, "/RPCTEST/rpc")

    def echo(a):
        return "echo: %r" % a

    rpcthr.register_rpc("echo", echo)

    rpcthr.start()
    print("Waiting 5s")
    time.sleep(5)
    print("Sending an echo RPC...")
    res = send_rpc(etcd_client, "/RPCTEST/rpc", "echo", MSG="abcdef")
    print("Result = %r" % res)
    print("Waiting 10s")
    time.sleep(10)
    print("Stopping RPCThread")
    rpcthr.stop()


