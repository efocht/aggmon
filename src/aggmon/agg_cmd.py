import argparse
import logging
import os
import re
import sys
import time
import json
from etcd_client import EtcdClient
from agg_component import ETCD_COMPONENT_PATH
from hierarchy_helpers import hierarchy_from_url
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


def subscribe(etcd_client, rpc_path, *args):
    if len(args) < 1:
        return None
    target = args[0]
    rest = args[1:]
    msg = dict_from_args(*rest)
    msg["TARGET"] = target
    result = send_rpc(etcd_client, rpc_path, "subscribe", **msg)
    return result


def unsubscribe(etcd_client, rpc_path, *args):
    if len(args) < 1:
        return None
    target = args[0]
    msg = {"TARGET": target}
    result = send_rpc(etcd_client, rpc_path, "unsubscribe", **msg)
    return result


def show_subscriptions(etcd_client, rpc_path):
    result = send_rpc(etcd_client, rpc_path, "show_subs")
    return result


def tag_add(etcd_client, rpc_path, *args):
    if len(args) < 2:
        return None
    tag_name = args[0]
    tag_value = args[1]
    rest = args[2:]
    msg = dict_from_args(*rest)
    msg["TAG_KEY"] = tag_name
    msg["TAG_VALUE"] = tag_value
    result = send_rpc(etcd_client, rpc_path, "add_tag", **msg)
    return result


def tag_remove(etcd_client, rpc_path, *args):
    if len(args) < 2:
        return None
    tag_key = args[0]
    tag_val = args[1]
    msg = {"TAG_KEY": tag_key, "TAG_VALUE": tag_val}
    result = send_rpc(etcd_client, rpc_path, "remove_tag", **msg)
    return result


def tags_reset(etcd_client, rpc_path):
    result = send_rpc(etcd_client, rpc_path, "reset_tags")
    return result

def tags_show(etcd_client, rpc_path):
    result = send_rpc(etcd_client, rpc_path, "show_tags")
    return result


def aggmon_cmd(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument('-C', '--component-type', default="", action="store", help="component type of the receiver")
    ap.add_argument('-H', '--hierarchy-url', default="", action="store", help="hierarchy URL of the receiver")
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

    rawp = sp.add_parser('raw',  help="Raw commands")
    rawp.add_argument('args', nargs='+', help="raw command and arguments, arguments coming as key value pairs. "
                      + "Example: test_rpc key1 val1 key2 val2")

    pargs = ap.parse_args(argv)

    log_level = eval("logging."+pargs.log.upper())
    FMT = "%(asctime)s %(levelname)-5.5s [%(name)s][%(threadName)s] %(message)s"
    logging.basicConfig( stream=sys.stderr, level=log_level, format=FMT )

    etcd_client = EtcdClient()
    hierarchy, key, path = hierarchy_from_url(pargs.hierarchy_url)
    rpc_path = ETCD_COMPONENT_PATH + "/" + pargs.component_type + "/" + hierarchy + "/" + key + "/rpc"

    if pargs.cmd_group == "tag":
        if pargs.add is not None:
            result = tag_add(etcd_client, rpc_path, *pargs.add)
        elif pargs.remove is not None:
            result = tag_remove(etcd_client, rpc_path, *pargs.remove)
        elif pargs.show:
            result = tags_show(etcd_client, rpc_path)

    elif pargs.cmd_group == "sub":
        if pargs.add is not None:
            result = subscribe(etcd_client, rpc_path, *pargs.add)
        elif pargs.remove is not None:
            result = unsubscribe(etcd_client, rpc_path, *pargs.remove)
        elif pargs.show:
            result = show_subscriptions(etcd_client, rpc_path)

    elif pargs.cmd_group == "raw":
        if pargs.args is not None:
            rpc_cmd = pargs.args[0]
            rpc_args = dict_from_args(*pargs.args[1:])
            result = send_rpc(etcd_client, rpc_path, rpc_cmd, **rpc_args)

    print json.dumps(result)

if __name__ == "__main__":
    aggmon_cmd(sys.argv[1:])
