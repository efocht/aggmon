import argparse
import logging
import os
import re
import sys
import time
import json
from etcd import *
from etcd_client import EtcdClient
from agg_component import hierarchy_from_url
from agg_config import Config, DEFAULT_CONFIG_DIR
from agg_rpc import send_rpc


log = logging.getLogger( __name__ )


def hierarchy_cmd(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument('-l', '--log', default="info", action="store", help="logging: info, debug, ...")
    action = ap.add_mutually_exclusive_group(required=True)
    action.add_argument('-L', '--list', default=False, action="store_true",
                        help="List configured hierarchies.")
    action.add_argument('-A', '--add', default=False, action="store_true",
                        help="Add hierarchy node.")
    action.add_argument('-D', '--delete', default=False, action="store_true",
                        help="Delete hierarchy node.")
    action.add_argument('-S', '--show', default=False, action="store_true",
                        help="Show hierarchy nodes at and under the node in --hierarchy-url.")
    
    ap.add_argument('-H', '--hierarchy-url', default="", action="store",
                    help="hierarchy URL of the receiver")
    ap.add_argument('-n', '--nodes', default="", action="store",
                    help="Comma separated list of nodes at the new hierarchy-url.")

    #ap.add_argument('args', nargs='+', help="Key value pairs to be stored at the hierarchy node.")

    pargs = ap.parse_args(argv)

    log_level = eval("logging."+pargs.log.upper())
    FMT = "%(asctime)s %(levelname)-5.5s [%(name)s][%(threadName)s] %(message)s"
    logging.basicConfig( stream=sys.stderr, level=log_level, format=FMT )

    #import pdb; pdb.set_trace()
    
    etcd_client = EtcdClient()
    if not etcd_client:
        print "Failed to connect to etcd!"
        sys.exit(1)
    config = Config(etcd_client)

    if pargs.list:
        for h in config.get("/hierarchy"):
            print h

    else:
        if not pargs.hierarchy_url:
            print "Argument missing: --hierarchy-url"
            sys.exit(1)

        hierarchy, key, hpath = hierarchy_from_url(pargs.hierarchy_url)
        conf_path = "/hierarchy/%(hierarchy)s/%(key)s" % locals()
        
        if pargs.show:
            try:
                hnode = config.get(conf_path)
            except Exception as e:
                log.error("Failed to retrieve hierarchy node %s : %r" % (pargs.hierarchy_url, e))
                sys.exit(1)
            print hnode

        elif pargs.delete:
            try:
                hnode = config.get(conf_path)
            except Exception as e:
                log.error("Failed to retrieve hierarchy node %s : %r" % (pargs.hierarchy_url, e))
                sys.exit(1)
            config.delete(conf_path)

        elif pargs.add:
            if not pargs.nodes:
                print "Argument missing: --nodes"
                sys.exit(1)
            nodes = pargs.nodes.split(",")
            try:
                hnode = config.get(conf_path)
            except EtcdKeyNotFound:
                pass
            except Exception as e:
                log.error("Error while checking node %s : %r" % (pargs.hierarchy_url, e))
                sys.exit(1)
            try:
                config.set(conf_path, {"nodes": nodes, "hpath": hpath})
            except Exception as e:
                log.error("Error while adding %s : %r" % (pargs.hierarchy_url, e))
                sys.exit(1)

if __name__ == "__main__":
    hierarchy_cmd(sys.argv[1:])
