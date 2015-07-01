import argparse
import logging
import os
import re
import sys
import time
import json
import zmq
from agg_rpc import send_rpc


log = logging.getLogger( __name__ )


def dict_from_args(*args):
    res = {}
    for i in xrange(len(args)/2):
        key = args[2*i]
        if len(args) >= 2*i + 2:
            value = args[2*i+1]
            res[key] = value
        else:
            break
    return res


def subscribe(context, server, *args):
    if len(args) < 1:
        return None
    target = args[0]
    rest = args[1:]
    msg = dict_from_args(*rest)
    msg["TARGET"] = target
    result = send_rpc(context, server, "subscribe", **msg)
    return result


def unsubscribe(context, server, *args):
    if len(args) < 1:
        return None
    target = args[0]
    msg = {"TARGET": target}
    result = send_rpc(context, server, "unsubscribe", **msg)
    return result


def show_subscriptions(context, server):
    result = send_rpc(context, server, "show_subs")
    return result


def tag_add(context, server, *args):
    if len(args) < 2:
        return None
    tag_name = args[0]
    tag_value = args[1]
    rest = args[2:]
    msg = dict_from_args(*rest)
    msg["TAG_KEY"] = tag_name
    msg["TAG_VALUE"] = tag_value
    result = send_rpc(context, server, "add_tag", **msg)
    return result


def tag_remove(context, server, *args):
    if len(args) < 2:
        return None
    tag_key = args[0]
    tag_val = args[1]
    msg = {"TAG_KEY": tag_key, "TAG_VALUE": tag_val}
    result = send_rpc(context, server, "remove_tag", **msg)
    return result


def tags_reset(context, server):
    result = send_rpc(context, server, "reset_tags")
    return result

def tags_show(context, server):
    result = send_rpc(context, server, "show_tags")
    return result



if __name__ == "__main__":

    ap = argparse.ArgumentParser()
    ap.add_argument('-C', '--cmd-port', default="tcp://127.0.0.1:5556", action="store", help="RPC command port")
    ap.add_argument('-l', '--log', default="info", action="store", help="logging: info, debug, ...")
    ap.add_argument('-v', '--verbose', default=False, action="store_true", help="verbosity")

    sp = ap.add_subparsers(dest='cmd_group', help="Subcommand help")

    tagp = sp.add_parser('tag',  help="Tagging commands")
    tagp.add_argument('--add', nargs=argparse.REMAINDER, metavar=('tagname', 'tagvalue', '[key value]'),
                      action="store", help="add a tagging condition")
    tagp.add_argument('--remove', '--del', nargs=2, metavar=('tagname', 'tagvalue'), action="store",
                      help="remove a tagging condition")
    tagp.add_argument('--show', default=False, action="store_true", help="show tags")

    subp = sp.add_parser('sub', help="Subscribe commands")
    subp.add_argument('--add', nargs=argparse.REMAINDER, metavar=('target', '[key value]'),
                      action="store", help="add a subscription to target and optional key value match conditions")
    subp.add_argument('--remove', '--del', nargs=1, metavar='target', action="store",
                      help="remove a target from the subscriptions")
    subp.add_argument('--show', default=False, action="store_true", help="show subscriptions")

    
    pargs = ap.parse_args()

    log_level = eval("logging."+pargs.log.upper())
    FMT = "%(asctime)s %(levelname)-5.5s [%(name)s][%(threadName)s] %(message)s"
    logging.basicConfig( stream=sys.stderr, level=log_level, format=FMT )

    context = zmq.Context()
    server = pargs.cmd_port

    if pargs.cmd_group == "tag":
        if pargs.add is not None:
            result = tag_add(context, server, *pargs.add)
        elif pargs.remove is not None:
            result = tag_remove(context, server, *pargs.remove)
        elif pargs.show:
            result = tags_show(context, server)

    elif pargs.cmd_group == "sub":
        if pargs.add is not None:
            result = subscribe(context, server, *pargs.add)
        elif pargs.remove is not None:
            result = unsubscribe(context, server, *pargs.remove)
        elif pargs.show:
            result = show_subscriptions(context, server)

    print "%r" % result

    context.destroy()
