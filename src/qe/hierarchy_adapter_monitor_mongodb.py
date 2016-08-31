#!/usr/bin/python

import copy
import json
from pymongo import MongoClient, ASCENDING
from bson.objectid import ObjectId

from collections import defaultdict
import re

#import monitor_hierarchy
from qe.base_types.container_object import ContainerObject

import parsing

import qe.queryengine.tokens as tokens
import qe.queryengine.operators as ops
import qe.queryengine.query_parser as query_parser
import qe.queryengine.selector_parser as selector_parser

from hierarchy_adapter import HierarchyAdapter
from metric_store.mongodb_store import MongoDBMetricStore
import pdb
from StringIO import StringIO
import sys


class LxClientException(Exception):
    pass


def get_find_condition( condition ):
    #print "get_find_condition start:", condition
    if isinstance( condition, ops.StringEqual ):
        if condition.invert:
            return {condition.attribute: {"$ne": condition.match}}
        else:
            return {condition.attribute: condition.match}

    elif isinstance( condition, ops.RegexpMatch ):
        if condition.invert:
            return {condition.attribute: {"$not": {"$regex": condition.pattern}}}
        else:
            return {condition.attribute: {"$regex": condition.pattern}}

    elif isinstance( condition, ops.Contains ):
        raise NotImplementedError
        #if condition.invert:
        #    return "( not (%s in '%s') )" % ( sql_escape_attr( condition.attribute ), sql_escape( condition.element ) )
        #else:
        #    return "(%s in '%s')" % ( sql_escape_attr( condition.attribute ), sql_escape( condition.element ) )

    elif isinstance( condition, ops.Binary ):
        if condition.operator == ops.Binary.AND:
            op = "$and"
        elif condition.operator == ops.Binary.OR:
            op = "$or"
        else:
            assert False, "unknown operator type %s" % condition.operator
        if condition.invert:
            return {"$not": {op: [get_find_condition(condition.condition1), get_find_condition(condition.condition2)]}}
        else:
            return {op: [get_find_condition(condition.condition1), get_find_condition(condition.condition2)]}
    else:
        assert False, "operator %s not implemented" % condition.__class__
    return {}


def _repl_func_by_name( value ):
    """
    This is a hack.
    TODO: Remove it and fix the problem in the right place (probably __get_all_attributes
    or another derived method).
    The problem is that object ATTRIBUTES can contain functions and simplejson
    can't serialize them. This function recurses over list or dict elements and
    replaces callables by the string 'function:' + <function name>.
    """
    val = copy.copy( value )
    if isinstance( val, list ):
        for i in range( 0, len( val ) ):
            val[i] = _repl_func_by_name( val[i] )
    elif isinstance( value, dict ):
        for k, v in val.items():
            val[k] = _repl_func_by_name( v )
    elif hasattr( val, '__call__' ):
        val = "function:" + val.__name__
    return val


def current_value_wrapper(hierarchy_adapter, _id, obj, *args):
    clname = obj["_type"]
    metric_name = obj["NAME"]
    host_name = obj["HOST"]
    val = hierarchy_adapter.store.current_value(metric_name=metric_name, host_name=host_name)
    #except Exception as e:
    #    raise LxClientException("%r" % e)
    return val


def time_series_wrapper(hierarchy_adapter, _id, obj, *args):
    # time_series( self, hpath, start_s=0, end_s=0, nsteps=sys.maxint, step_s=0 ):
    hpath = obj["hpath"]
    if len(args) > 0:
        args = [ int(arg) for arg in args]
    val = hierarchy_adapter.store.time_series(hpath, *args)
    return val


def tree_changed(hierarchy_adapter, _id, obj, *args):
    #
    # TODO: if this needs to be solved, there is a problem. We need the past state in order to
    # detect whether anything changed. But this adapter should actually be stateless.
    # We could use the time encoded in the record ID of the last record in the metadata coillection, somehow.
    #
    #val = hierarchy_adapter.store.last_md()
    # return False for now, means, no change in the hierarchy. This is a wrong fake result.
    return False

#
# moves this away from here ASAP!
#

class MGroup(ContainerObject):
    ATTRIBUTES = {
        "is_dirty": { "function": tree_changed}
    }
    CONTAINS_TYPES = ["MMetric", "MGroup", "MHost"]

class MHost(ContainerObject):
    ATTRIBUTES = {}
    CONTAINS_TYPES = ["MMetric"]

class MMetric(ContainerObject):
    ATTRIBUTES = {
        "current_value": { "function": current_value_wrapper},
        "time_series": { "function": time_series_wrapper}
    }
    CONTAINS_TYPES = []


class HierarchyAdapterMonitorMongoDB( HierarchyAdapter ):

    def __init__( self, config ):
        self.qp = parsing.Lr( query_parser.spec )
        self.sp = parsing.Lr( selector_parser.spec )
        host = config["host"]
        port = config["port"]
        user = config["user"]
        dbname = config["database"]
        passwd = config["password"]
        self.store = MongoDBMetricStore(host_name=host, port=port, db_name=dbname, username=user, password=passwd)

        factory = {}
        def _search_subclasses( base ):
            for c in base.__subclasses__():
                factory[c.__name__] = c
                _search_subclasses( c )
        _search_subclasses( ContainerObject )
        self.factory = factory


    def oid_to_rid(self, oid):
        return ObjectId(oid)


    def rid_to_oid(self, rid):
        return str(rid)


    def cleanup_content_get( self, content ):
        clean_content = content.copy()
        clean_content["_id"] = self.rid_to_oid( content["_id"] )
        return clean_content

    def _parse_query( self, arg ):
        self.qp.reset()
        stream = StringIO( arg )
        lexer = QueryLexer( self.qp, stream )
        return lexer.parse().value


    def _parse_selector( self, arg ):
        self.sp.reset()
        stream = StringIO( arg )
        lexer = QueryLexer( self.sp, stream )
        return lexer.parse().value


    def client( self, op, *args ):
        if op == "describe":
            if len(args) == 0:
                return {}
            return self.op_describe( args[0] )
        elif op == "select":
            if len(args) == 0:
                return []
            query = self._parse_query( args[0] )
            if len(args) > 1:
                selector = self._parse_selector( args[1] )
            else:
                selector = None
            return self.op_select( query, selector )
        elif op == "locate":
            if len(args) == 0:
                return []
            query = self._parse_query( args[0] )
            return self.op_locate( query )


    def op_describe( self, key ):
        """
        Return object's class metadata information.
        """
        if key in self.factory:
            # class name was provided
            cls = self.factory[key]
        else:
            # try locating this obj_id
            try:
                obj = self.store.find_md( {"_id": self.oid_to_rid(key)} )[0]
            except Exception as e:
                print "DESCRIBE: locating _id=%s failed. %r" % (key, e)
                #log.warning("DESCRIBE: locating _id=%s failed. %r" % (key, e))
                return False
            cls = self.factory[obj["_type"]]
        result = {}
        for var in ["ATTRIBUTES", "CONTAINS_TYPES", "KEY_ATTRIBUTE"]:
            if hasattr( cls, var ):
                result[var] = _repl_func_by_name( getattr( cls, var ) )
        # is this needed?
        result["_type"] = cls.__name__
        return result


    def op_locate( self, query ):
        selector = self._parse_selector( "[_id]" )
        return [ self._process_selector( el, selector )["_id"] for el in self._do_query( query ) ]


    def op_select( self, query, selector ):
        if selector is None:
            return [ self.cleanup_content_get( el ) for el in self._do_query( query ) ]
        else:
            return [ self._process_selector( el, selector ) for el in self._do_query( query ) ]


    def _process_selector( self, obj, selector, strictly_selected=False ):
        clname = obj["_type"]
        res = {}
        if not strictly_selected:
            res = { "_id": self.rid_to_oid( obj["_id"] ), "_type": clname }
        c = self.factory[clname]
        clmd = c.ATTRIBUTES
        for attr in selector:
            a = attr.name
            if a in res:
                continue
            elif a == "_id":
                res[a] = self.rid_to_oid( obj["_id"] )
                continue
            value = obj.get( a, None )
            if value is not None:
                res[a] = value
            else:
                attrmd = clmd.get( a, None )
                if attrmd is not None:
                    func = attrmd.get( "function", None )
                    if func is not None:
                        args = []
                        if attr.arguments is not None:
                            args = [ str(arg.value) for arg in attr.arguments ]
                        res[a] = func( self, self.rid_to_oid(obj["_id"]), obj, *args )
                    else:
                        res[a] = None
        return res


    def _do_query( self, query ):
        ( id_list, path ) = query
        #print id_list, path
        find = None
        if id_list is not None:
            if isinstance( id_list, query_parser.OIDList ):
                if len(id_list.value) == 1:
                    find = {"_id": id_list.value[0].value}
                else:
                    find = {"_id": {"$in": [i.value for i in id_list.value]}}
            else:
                # This is a list of hpaths or query expressions
                objs = []
                for q in id_list.value:
                    objs.extend( self._do_query( q.value ) )
                if path is None:
                    return objs
                # TODO: or not?
                if len(objs) == 1:
                    find = {"_id": objs[0]["_id"]}
                else:
                    ids_list = [ obj["_id"] for obj in objs ]
                    find = {"_id": {"$in": ids_list}}

        objs = []
        hpaths = []
        if find is not None:
            if path is None:
                return self.store.find_md( find )
            # TODO: don't use collection here... but not so urgent
            hpaths = self.store._col_md.distinct("hpath", query=find)

        def append_hpath(hpaths, data):
            #print "append_hpath start: data=%r, hpaths=%r" % (data, hpaths)
            if len(hpaths) == 0:
                hpaths.append(data)
            else:
                for i in xrange(len(hpaths)):
                    hpaths[i] += data
            #print "append_hpath end: hpaths=%r" % hpaths
            return hpaths

        def strip_hpath(hpaths, levels=1):
            #print "strip_hpath start: hpaths=%r" % hpaths
            for i in xrange(len(hpaths)):
                hpath = hpaths[i].lstrip("/")
                elems = hpath.split("/")
                hpaths[i] = "/" + "/".join(elems[:len(elems) - levels])
            #print "strip_hpath end: hpaths=%r" % hpaths
            return hpaths

        def expand_tripledot(hpaths):
            new_hpaths = []
            for hpath in hpaths:
                while hpath != "/":
                    hpath = strip_hpath([hpath])[0]
                    new_hpaths.append(hpath)
            return new_hpaths

        def resolve_hpaths(hpaths, has_regexp, condition, full=False):
            #print "resolve_hpaths start:\nhpaths=%r\nhas_regexp=%r\ncondition=%r\nfull=%r" % \
            #      (hpaths, has_regexp, condition, full)
            objs = []
            proj = None
            if not full:
                proj = {"hpath": True}
            if not has_regexp:
                if condition:
                    objs = self.store.find_md({"$and": [condition, {"hpath": {"$in": hpaths}}]}, proj)
                else:
                    objs = self.store.find_md({"hpath": {"$in": hpaths}}, proj)
            else:
                ors = []
                for hpath in hpaths:
                    ors.append({"hpath": {"$regex": "^" + hpath + "$"}})
                if condition:
                    objs = self.store.find_md({"$and": [condition, {"$or": ors}]}, proj)
                else:
                    objs = self.store.find_md({"$or": ors}, proj)
            #print "resolve_hpaths end: _Cursor__spec: %r" % objs._Cursor__spec
            if objs.count() == 0:
                return None
            if not full:
                return [obj["hpath"] for obj in objs]
            return [obj for obj in objs]


        has_regexp = False
        arbitrary_depth = False
        cond = {}
        if path is not None:
            #for el in path.value:
            for i in xrange(len(path.value)):
                el = path.value[i]
                #print "el = ", el
                #pdb.set_trace()
                if not el.is_filter:
                    if el.name is None:
                        # // encountered
                        if i == len(path.value) - 1:
                            hpaths = ["/"]
                        arbitrary_depth = True

                    elif isinstance( el.name, tokens.Dot ):
                        arbitrary_depth = False

                    # .. found
                    elif isinstance( el.name, tokens.DoubleDot ):
                        if arbitrary_depth:
                            #  //..
                            raise NotImplementedError
                        if has_regexp:
                            hpaths = resolve_hpaths(hpaths, has_regexp, cond, full=False)
                            has_regexp = False
                        hpaths = strip_hpath(hpaths)
                        arbitrary_depth = False

                    elif isinstance( el.name, tokens.TripleDot ):
                        hpaths = expand_tripledot(hpaths)
                        arbitrary_depth = False

                    elif isinstance( el.name, tokens.Asterisk ):
                        if arbitrary_depth:
                            hpaths = append_hpath(hpaths,  "/.*" )
                        else:
                            hpaths = append_hpath(hpaths,  "/[^/]+" )
                        arbitrary_depth = False
                        has_regexp = True # force resolving

                    else:
                        # element it is a string
                        if el.name in self.factory:
                            assert False, "Query for class %s without '[]'" % el.name
                        if arbitrary_depth:
                            hpaths = append_hpath(hpaths,  "/.*" )
                            has_regexp = True
                        hpaths = append_hpath(hpaths,  "/" + el.name )
                        arbitrary_depth = False

                else:
                    #
                    # Class[condition]
                    #
                    if el.condition is None:
                        cond = {}
                    else:
                        cond = get_find_condition( el.condition )
                        #print "condition = %r" % cond

                    if isinstance( el.name, tokens.Dot ):
                        pass

                    elif isinstance( el.name, tokens.DoubleDot ):
                        if arbitrary_depth:
                            raise NotImplementedError
                        if has_regexp:
                            hpaths = resolve_hpaths(hpaths, has_regexp, cond, full=False)
                            has_regexp = False
                        hpaths = strip_hpath(hpaths)
                        if cond:
                            has_regexp = True
                        arbitrary_depth = False

                    elif isinstance( el.name, tokens.TripleDot ):
                        hpaths = expand_tripledot(hpaths)
                        arbitrary_depth = False
                        has_regexp = True # force resolving the condition

                    elif isinstance( el.name, tokens.Asterisk ):
                        if arbitrary_depth:
                            hpaths = append_hpath(hpaths,  "/.*" )
                        else:
                            hpaths = append_hpath(hpaths,  "/[^/]+" )
                        arbitrary_depth = False
                        has_regexp = True # force resolving the condition

                    else:
                        # TODO: add Class match
                        if el.name not in self.factory:
                            assert False, "ERROR: match for unsupported object type '%s'!" % el.name
                        if arbitrary_depth:
                            hpaths = append_hpath(hpaths,  "/.*" )
                        else:
                            hpaths = append_hpath(hpaths,  "/[^/]+" )
                        has_regexp = True
                        if cond:
                            cond = {"$and": [cond, {"_type": el.name}]}
                        else:
                            cond = {"_type": el.name}
                    arbitrary_depth = False
                if has_regexp:
                    #
                    # and now get the results!
                    #
                    if i==len(path.value)-1:
                        objs = resolve_hpaths(hpaths, has_regexp, cond, full=True)
                        break
                    else:
                        hpaths = resolve_hpaths(hpaths, has_regexp, cond, full=False)
                        if hpaths is None:
                            break
                    has_regexp = False

        if objs is None or hpaths is None:
            return []
        if len(objs) == 0 and len(hpaths) > 0:
            objs = resolve_hpaths(hpaths, has_regexp, {}, full=True)
        #pdb.set_trace()
        return [obj for obj in objs]


if __name__ == "__main__":
    #
    # Example:
    #   python hierarchy_adapter_monitor_mongodb.py select /universe/tb001/servers.tb001.cpu.cpu1.system [hpath]
    #
    
    from qe.queryengine.query_lexer import *

    # testing...
    config = {"host": "localhost", "port": 27017, "user": "", "password": "",
              "database": "metricdb", "md_collection": "metric_md"}

    ha = HierarchyAdapterMonitorMongoDB(config)

    print ha.client(sys.argv[1], *sys.argv[2:])

