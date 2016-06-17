##########################################################################
#                                                                        #
#               Copyright (C) 2014 - 2015 NEC HPC Europe.                #
#                                                                        #
#  These coded instructions, statements, and computer programs  contain  #
#  unpublished  proprietary  information of NEC HPC Europe, and are      #
#  are protected by Federal copyright law.  They  may  not be disclosed  #
#  to  third  parties  or copied or duplicated in any form, in whole or  #
#  in part, without the prior written consent of NEC HPC Europe.         #
#                                                                        #
##########################################################################
#
import sys
import time
import datetime
import logging
from pymongo import MongoClient, ASCENDING, DESCENDING
from bson.code import Code
from bson.objectid import ObjectId
from metric_store import MetricStore

__all__ = ["MongoDBMetricStore", "MongoDBJobList", "MongoDBJobStore", "MongoDBStatusStore"]

# Constants
MAX_RECORDS = 1500
METRIC_TTL = 900
PARTITION_TIMEFRAME = 60 * 60 * 24 * 30

log = logging.getLogger( __name__ )


class MongoDBStore(object):
    """
    Base class for different types of metric storage in MongoDB database.

    host_name and port could be something like:
        "vmn1:27017"
        "vmn1", 27017
        ["vmn1:27017", "vmn2:27017"]
        "mongodb://vmn1:27017,vmn2:27017"

    """
    def __init__( self, hostname="localhost", port=None, db_name="metric", username="", password="" ):
        if port and ( isinstance( port, int ) or isinstance( port, basestring ) ):
            self.port = int( port )
        else:
            self.port = 27017
        self.db_name = db_name
        self.username = username
        self.password = password
        self._client = MongoClient( hostname, self.port )
        self.db = self._client[self.db_name]
        if self.username and self.password:
            self.db.authenticate(self.username, self.password, mechanism='MONGODB-CR')

    def _update_partition( self, name, partition_timeframe=PARTITION_TIMEFRAME ):
        """
        Check for partitioned collections if current partition is older than PARTITION_TIMEFRAME
        abd if so add a new partition.
        """
        partition_timeframe = PARTITION_TIMEFRAME if partition_timeframe < 0 else partition_timeframe
        attrib = "partitioned"
        col = self.db[name]
        eval_str = "db." + name + ".stats()" + "." + attrib
        try:
            partitioned = self.db.eval( eval_str )
        except Exception, e:
            raise Exception( "Failed to query partitioned attribute, %s" % str( e ) )
        if partitioned:
            # check if creation time of newest partition is older than now minus partition_timeframe
            eval_str = "db." + name + ".getPartitionInfo().partitions["
            eval_str += "db." + name + ".getPartitionInfo().numPartitions-1].createTime-0"
            try:
                creation_time = int( self.db.eval( eval_str ) / 1000 )
            except Exception, e:
                raise Exception( "Failed getting partition info, %s" % str( e ) )
            #print "creation_time:", creation_time, "now:", int( time.time() ), "partition_timeframe:", partition_timeframe
            if creation_time < int( time.time() ) - partition_timeframe:
                # create new partition (only possible in Percona aka TokuMX)
                try:
                    eval_str = "db." + name + ".addPartition()"
                    self.db.eval( eval_str )
                except Exception, e:
                    raise Exception( "Failed to add partition, %s" % str( e ) )

    def get_collection( self, name, arg_str="" ):
        """
        e.g. invocation
            db.get_collection( "log", "{capped: true, size: 5242880, max: 5000}" )
            db.get_collection( "metric_universe", "{partitioned: true, primaryKey: {T: 1, _id: 1}}" )

        Note: The second argument must be a string that contains vaild JavaScript!
        """
        if name not in self.db.collection_names():
            # create collection with provided args 
            eval_str = "db.createCollection(\"" + name + "\""
            eval_str += ", " + arg_str if arg_str else ""
            eval_str += ")"
            try:
                self.db.eval( eval_str )
                col = self.db[name]
            except Exception, e:
                raise Exception("Failed to create collection, %s" % str( e ))
        else:
            # collection exists, update it if required
            self._update_partition( name )
        col = self.db[name]
        return col

    def close( self ):
        self._client.fsync()
        self._client.close()


class MongoDBJobList(MongoDBStore):
    """
    List of currently running Jobs.
    """
    def __init__( self, col_name="job_list", **kwds ):
        MongoDBStore.__init__( self, **kwds )
        self.__col_job_list = self.get_collection( col_name )
        self.__col_job_list.ensure_index( [("name", ASCENDING)], unique=True )

    def addJob( self, metric ):
        try:
            self.__col_job_list.update( {"name": metric["name"]},
                                        metric, multi=False, upsert=True )
        except Exception, e:
            raise Exception( "Failed to add Job to list: %s" % str( e ) )

    def removeJob( self, jobid ):
        try:
            self.__col_job_list.remove( {"name": jobid} )
        except Exception, e:
            raise Exception( "Failed to remove Job from list: %s" % str( e ) )

    def find( self, match=None ):
        jobs = None
        try:
            jobs = self.__col_job_list.find( match )
        except Exception, e:
            raise Exception("Failed to retrieve Job list, %s" % str( e ))
        return jobs


class MongoDBMetricStore(MongoDBStore, MetricStore):
    """
    Numerical (Ganglia style) metrics
    Note: In Percona / TokuMX partitioned collections are used.
    """
    def __init__(self, hostname="localhost", port=27017, db_name="metric", username="", password="", group="/universe",
                 md_col="metric_md", val_col="metric", val_ttl=3600*24*180, **kwds):
        MongoDBStore.__init__( self, hostname=hostname, port=port, db_name=db_name, username=username, password=password )
        self.group = group
        self._col_md = self.get_collection( md_col )
        self._col_md_name = md_col
        self._col_val_base = val_col
        self._col_val_name = val_col + "_" + MetricStore.group_suffix( group )
        self._col_val = self.get_collection( self._col_val_name, "{partitioned: true, primaryKey: {T: 1, _id: 1}}" )
        self._val_ttl = val_ttl
        try:
            # index for metadata
            self._col_md.ensure_index( [("hpath", ASCENDING)], unique=True )
            # indices for values
            self._col_val.ensure_index( [("H", ASCENDING),
                                         ("N", ASCENDING),
                                         ("T", ASCENDING)],
                                        unique=False )
            #self._col_val.ensure_index( [("T", ASCENDING)],
            #                            unique=False, background=True,
            #                            expireAfterSeconds=self._val_ttl )
            self._col_val.ensure_index( [("J", ASCENDING)], unique=False )
        except Exception, e:
            raise Exception( "Failed to ensure index: %s" % str( e ) )
        MetricStore.__init__( self )

    def update_partitions( self, partition_timeframe=-1, **kwds ):
        """
        Call update on all collections in MongoDBMetricStore.
        """
        for name in [self._col_md_name, self._col_val_name]:
            self._update_partition( name=name, partition_timeframe=partition_timeframe, **kwds  )

    def insert_md( self, md ):
        hpath = md["CLUSTER"]
        group_name = md["CLUSTER"].split("/")[-1]
        group = {"NAME": group_name, "hpath": hpath, "_type": "MGroup"}
        spec = {"NAME": md["NAME"], "HOST": md["HOST"], "CLUSTER": md["CLUSTER"]}
        self._col_md.update( {"hpath": hpath}, {"$set": group}, upsert=True )

        if md["HOST"] != "":
            hpath += "/" + md["HOST"]
            host = {"NAME": md["HOST"], "hpath": hpath, "_type": "MHost"}
            self._col_md.update( {"hpath": hpath}, {"$set": host}, upsert=True )

        hpath += "/" + md["NAME"]
        md["hpath"] = hpath
        md["_type"] = "MMetric"
        return self._col_md.update( {"hpath": hpath}, {"$set": md}, upsert=True )

    def get_md( self ):
        return self.find_md( match={"CLUSTER": self.group} )

    def find_md( self, match=None, proj=None ):
        return self._col_md.find( match, proj )

    def insert_val( self, metric ):
        ## EF: we switch to integer values for the time, i.e. second granularity
        ##     the datetime type metric takes up too much space in mongodb.
        ##     Might be that we need to expire old records manually.
        ## make sure the time has proper format such that TTL will expire it eventually
        #metric["T"] = datetime.datetime.fromtimestamp(metric["T"])
        return self._col_val.insert( metric )

    def insert( self, val ):
        if "SLOPE" in val:
            # this must be a metadata record coming from gmond/ganglia
            # is this in cache?
            _type = self.md_cache.get(val["HOST"], val["NAME"])
            if _type is not None and _type == val["TYPE"]:
                log.debug( "skipping MD insert for host=%s, metric=%s" % (val["HOST"], val["NAME"]) )
                return
            val["CLUSTER"] = self.group
            res = self.insert_md(val)
            log.debug("insert_metadata returned: %r" % res)
            if "upserted" in res:
                log.debug( "feeding cache:", val["HOST"], val["NAME"], res["upserted"], val["TYPE"] )
                self.md_cache.set(val["HOST"], val["NAME"], val["TYPE"])
            log.debug( "upserted %r" % val )
        else:
            # is this in cache?
            _type = self.md_cache.get(val["H"], val["N"])
            if _type is None:
                log.warn( "WARNING: metadata not found for this value record: %r" % val )
                #
                # make a metadata entry
                #
                v = val["V"]
                _type = "string"
                if isinstance(v, int):
                    _type = "int32"
                elif isinstance(v, float):
                    _type = "float"
                elif isinstance(v, list):
                    _type = "array"
                md = {"HOST": val["H"], "NAME": val["N"], "TYPE": _type, "CLUSTER": self.group}
                res = self.insert_md(md)
                log.debug("insert_metadata returned: %r" % res)
                if "upserted" in res:
                    log.debug( "feeding cache: HOST=%s, NAME=%s, TYPE=%s, %s" % (str( md["HOST"] ), str( md["NAME"] ), str( md["TYPE"] ), str( res["upserted"] )) )
                    self.md_cache.set(md["HOST"], md["NAME"], md["TYPE"])

            _time = self.v_cache.get(val["H"], val["N"])
            if _time is not None and _time == val["T"]:
                log.debug( "skipping V insert for host=%s, metric=%s: duplicate record for time %r" % (val["H"], val["N"], val["T"]) )
                return

            #string|int8|uint8|int16|uint16|int32|uint32|float|double
            if _type in ("int8", "uint8", "int16", "uint16", "int32", "uint32"):
                if not isinstance(val["V"], int):
                    val["V"] = int(val["V"])
            elif _type in ("float", "double"):
                if not isinstance(val["V"], float):
                    val["V"] = float(val["V"])
            res = self.insert_val(val)
            log.debug( "inserted %r" % val )
            self.v_cache.set(val["H"], val["N"], val["T"])


    def find_val( self, match=None, proj=None, sort=None, limit=0 ):
        return self._col_val.find( match, projection=proj, limit=limit, sort=sort )


    def drop_all( self ):
        if self.db:
            for col in self.db.collection_names():
                if col == self._col_md_name or \
                   col == self._col_val_name:
                    self.db[col].drop()


    #
    # TODO: finish implementation: arguments, transformation of data (nsteps)
    #
    def time_series( self, hpath, start_s=0, end_s=0, nsteps=sys.maxint, step_s=0 ):
        if end_s == 0 or end_s is None:
            end_s = sys.maxint
        if start_s is None:
            start_s = 0
        records = []
        # check if metric exists
        m = self.find_md( {"hpath": hpath} )
        if m.count() == 0:
            return records
        metric = m[0]

        host_name = metric["HOST"]
        metric_name = metric["NAME"]

        if not metric:
            return records
        match = {"$and": [{"H": host_name}, {"N": metric_name}, {"T": {"$gt": start_s, "$lt": end_s}}]}
        proj = {"T": True, "V": True}
        try:
            records = [[r["T"], r["V"]] for r in self._col_val.find( match, proj=proj )]
        except Exception, e:
            raise Exception("Query failed, %s" % str( e ))
        return records


    def current_value( self, metric_name=None, host_name=None ):
        if not isinstance( metric_name, basestring ) or not isinstance( host_name, basestring ):
            return None
        match = {"$and": [{"N": metric_name}, {"H": host_name}]}
        sort = [("T", DESCENDING)]
        limit = 1
        #proj = {"V": True, "_id": False}
        val = self.find_val( match, sort=sort, limit=limit )[0]
        return val["V"]


    def percentiles( self, metric_name=None, host_names=None, time_s=None, dmax=(15 * 60)):
        if not isinstance( metric_name, basestring ) or not isinstance( host_names, list ):
            return None
        if not time_s:
            time_s = time.time()

        pipeline = """[
            {$match: {$and : [ {N: "%s"}, {H: {$in: %s}}, {T: {$gt: %d}}, {T: {$lte: %d}}]}},
            {$sort: {T: 1}},
            {$group: {_id: "$H", value: {$last: "$V"}}},
            {$project: {_id: 0, value: 1}}
        ]""" % (metric_name, str( host_names ), time_s - dmax, time_s)

        finalize = """
                function(v) {
                    Q = 10;
                    // sort values, calculate quantiles, return array of quantiles
                    v = v.sort(function(a, b){return a.value-b.value});
                    var quant = [];
                    for (var q=1; q <= Q; q++) {
                        var p = Math.round(q / Q * v.length);
                        quant[q-1] = v[p-1].value;
                    };
                    return quant;
                };
        """
        return self.aggregate( pipeline, finalize )


    def find( self, match=None ):
        return self.aggregate( match )


    def aggregate( self, pipeline=None, finalize="0" ):
        if not isinstance( pipeline, basestring ):
            return None
        self.__db.system_js.agg = """
            function(pipeline, finalize) {
                // select relevant collections and apply pipeline operation, concatenate results
                var vals = [];
                var cols = db.getCollectionNames(); // TODO: filter for value collections here?
                for (i=0; i < cols.length; i++) {
                    if (cols[i].indexOf("system.") < 0) {
                        vals = vals.concat(db[cols[i]].aggregate(pipeline).result);
                    };
                };
                // run finalize function
                if (finalize instanceof Function) {
                    return finalize(vals);
                } else {
                    return vals;
                };
            };
        """
        return self.__db.system_js.agg( Code( pipeline ), Code( finalize ) )



class MongoDBJobStore(MongoDBStore):
    """
    Job metrics
    """
    def __init__( self, group="/universe", col="job", val_ttl=3600*24*360, **kwds ):
        MongoDBStore.__init__( self, **kwds )
        self._group = group
        self._col_base = col
        self._col_name = col + "_" + MetricStore.group_suffix( group )
        self._col = self.get_collection( self._col_name )
        self._col_ttl = val_ttl
        try:
            # indices for job data
            self._col.ensure_index( [("name", ASCENDING), ("value", ASCENDING)], unique=True )
            #self._col.ensure_index( [("name", ASCENDING), ("value", ASCENDING)],
            #                       unique=True, background=True, expireAfterSeconds=self._col_ttl )
        except Exception, e:
            raise Exception( "Failed to ensure index for collection %s: %s" % (self._col, str( e )) )

    def insert(self, metric):
        ## EF: we switch to integer values for the time, i.e. second granularity
        ##     the datetime type metric takes up too much space in mongodb.
        ##     Might be that we need to expire old records manually.
        ## make sure the time has proper format such that TTL will expire it eventually
        #metric["time"] = datetime.datetime.fromtimestamp( metric["time"] )
        return self._col.update( {"name": metric["name"], "value": metric["value"]}, metric, upsert=True )


    def find( self, match=None, proj=None ):
        return self._col.find( match, proj )


    def drop_all( self ):
        if self.db:
            for col in self.db.collection_names():
                if col == self._col_name: 
                    self.db[col].drop()


class MongoDBStatusStore(MongoDBStore):
    """
    Status (log style) metrics. Mainly originating from Nagios.
    """
    def __init__( self, group="/universe", col="status", val_ttl=3600*24*360, **kwds ):
        MongoDBStore.__init__( self, **kwds )
        self._group = group
        self._col_base = col
        self._col_name = col + "_" + MetricStore.group_suffix( group )
        self._col = self.get_collection( self._col_name )
        self._col_ttl = val_ttl
        try:
            # indices for status data
            self._col.ensure_index( [("name", ASCENDING),
                                     ("time", DESCENDING),
                                     ("host", ASCENDING)],
                                    unique=True )
            #self._col.ensure_index( [("name", ASCENDING), ("time", DESCENDING), ("host", ASCENDING)], unique=True, background=True, expireAfterSeconds=self._col_ttl )
        except Exception, e:
            raise Exception("Failed to ensure index for collection %s: %s" % (self._col, str(e)))

    def insert(self, metric):
        return self._col.update( {"name": metric["name"],
                                  "host": metric["host"],
                                  "time": metric["time"]},
                                 metric,
                                 upsert=True )


    def find( self, match=None, proj=None ):
        return self._col.find( match, proj )


    def drop_all( self ):
        if self.db:
            for col in self.db.collection_names():
                if col == self._col_name:
                    self.db[col].drop()



#
# Below is the original version of the MetricStore, for reference.
# TODO: delete once the new version is implemented.
#
from pymongo.son_manipulator import AutoReference, NamespaceInjector
class MongoDBMetricStoreOrig(MongoDBStore):
    def __init__( self, col_log_metadata="log_metadata", col_job_metric="job_metric",
                  ts_log_prefix="logts", **kwds ):
        MongoDBStore.__init__( self, **kwds )
        self.__col_log_metadata = self._db[col_log_metadata]
        self.__col_job_metric = self._db[col_job_metric]
        self._db.add_son_manipulator( NamespaceInjector() )
        self._db.add_son_manipulator( AutoReference( self._db ) )
        self.__col_log_metadata_name = col_log_metadata
        self.__col_job_metric_name = col_job_metric
        self.__ts_log_prefix = ts_log_prefix
        try:
            self.__col_log_metadata.ensure_index( [("host", ASCENDING),
                                                   ("name", ASCENDING)],
                                                  unique=True )
            self.__col_job_metric.ensure_index( [("host", ASCENDING),
                                                 ("name", ASCENDING),
                                                 ("value", ASCENDING)],
                                                unique=True )
        except Exception, e:
            raise Exception( "Failed to ensure index: %s" % str( e ) )

    def addMetric( self, new_metric ):
        if isinstance( new_metric, NMetric ):
            last_metric = self.getLastMetricByMetricName( new_metric["host"], new_metric["name"] )
            if last_metric is None:
                # insert new log metric, create new TS collection, insert TS Record, insert metric incl. DBRef
                record = TSLOGRecord( **new_metric["ts_record"] )
                metric = NMetric( host=new_metric["host"], name=new_metric["name"] )
                try:
                    _id = self.__col_log_metadata.insert( metric )
                    self.__db.create_collection( self.__ts_log_prefix + str( _id ),
                                                 size=(256 * MAX_RECORDS), max=MAX_RECORDS, capped=True )
                    metric["ts_record"] = record
                    metric["_id"] = _id
                    self.__db[self.__ts_log_prefix + str( _id )].insert( record )
                    self.__col_log_metadata.save( metric )
                except Exception, e:
                    raise Exception( "Failed to insert Nagios metric: %s" % str( e ) )
            else:
                # check if metric TTL exceeded and insert intermediate record if so
                if last_metric["ts_record"]["time"] < new_metric["ts_record"]["time"] - METRIC_TTL:
                    record = last_metric["ts_record"]
                    record["lastvalue"] = record["value"]
                    record["value"] = "UNKNOWN"
                    record["time"] = last_metric["ts_record"]["time"] + METRIC_TTL
                    record["output"] = "Metric TTL exceeded! Record inserted on %i." % int( time.time() )
                    del record["_id"]
                    _id = last_metric["_id"]
                    try:
                        record["_id"] = self.__db[self.__ts_log_prefix + str( _id )].insert( record )
                        self.__col_log_metadata.save( last_metric )
                    except Exception, e:
                        raise Exception( "Failed to insert intermediate Nagios record: %s" % str( e ) )
                if last_metric["ts_record"]["value"] != new_metric["ts_record"]["value"] or \
                   last_metric["ts_record"]["output"] != new_metric["ts_record"]["output"]:
                    # insert new log TS Record, update metric DBRef
                    record = TSLOGRecord( **new_metric["ts_record"] )
                    metric = NMetric( host=new_metric["host"], name=new_metric["name"] )
                    _id = last_metric["_id"]
                    metric["ts_record"] = record
                    metric["_id"] = _id
                    try:
                        self.__db[self.__ts_log_prefix + str( _id )].insert( record )
                        self.__col_log_metadata.save( metric )
                    except Exception, e:
                        raise Exception( "Failed to insert Nagios record: %s" % str( e ) )
                else:
                    # update time in TS Record
                    try:
                        _id = last_metric["_id"]
                        recid = last_metric["ts_record"]["_id"]
                        update = new_metric["ts_record"]["time"]
                        self.__db[self.__ts_log_prefix +
                                  str( _id )].update({"_id": recid}, {"$set": {"time": update}})
                    except Exception, e:
                        raise Exception( "Failed to update time in Nagios record: %s" % str( e ) )
        elif isinstance( new_metric, JMetric ):
            try:
                new_metric_key = {"host": new_metric["host"],
                                  "name": new_metric["name"],
                                  "value": new_metric["value"]}
                self.__col_job_metric.update( new_metric_key, new_metric, upsert=True )
            except Exception, e:
                raise Exception( "Failed to upsert Job metric: %s" % str( e ) )
        else:
            raise Exception( "Unsupported metric type: '%s'!" % (type( new_metric) ) )

    def clearMetrics( self ):
        if self.__db:
            for col in self.__db.collection_names():
                if col == self.__col_log_metadata_name or \
                   col.starts_with( self.__ts_log_prefix ) or \
                   col == self.__col_job_metric_name:
                    self.__db[col].drop()

    def getHostNames( self ):
        try:
            hosts_log = list( self.__col_log_metadata.distinct( "host" ) )
            hosts_job = list( self.__col_job_metric.distinct( "host" ) )
        except Exception, e:
            raise Exception("Query failed, %s" % str( e ))
        return list( set( hosts_log + hosts_job ) )

    def getLastMetricsByHostName( self, host_name ):
        # TODO: replace loop by a smarter query
        metrics = []
        try:
            for name in self.getMetricNames( host_name ):
                metrics.extend( [NMetric( **(m.to_dict()) ) for m in self.__col_log_metadata.find(
                                {"name": name, "host": host_name},
                                ).sort( "time", 1 ).limit( 1 )] )
        except Exception, e:
            raise Exception("Query failed, %s" % str( e ))
        return metrics

    def getLastMetricByMetricName( self, host_name, metric_name ):
        # TODO: check is as_class attribute could be used
        metric = None
        try:
            c = self.__col_log_metadata.find( {"name": metric_name, "host": host_name} ).limit( 1 )
            if c.count() > 0:
                d = c[0].to_dict()
                metric = NMetric( **d )
        except Exception, e:
            raise Exception("Query failed, %s" % str( e ))
        return metric

    def getMetricNames( self, host_name ):
        names = None
        try:
            names = self.__col_log_metadata.find( {"host": host_name}, {"name": 1} ).distinct( "name" )
        except Exception, e:
            raise Exception("Query failed, %s" % str( e ))
        return names

    def getRecordsByMetricName( self, host_name, metric_name, start_s=0, end_s=0, nsteps=sys.maxint, step_s=0 ):
        if end_s == 0 or end_s is None:
            end_s = sys.maxint
        if start_s is None:
            start_s = 0
        records = []
        metric = self.getLastMetricByMetricName( host_name, metric_name )
        if not metric:
            return records
        _id = metric["_id"]
        try:
            records = [TSLOGRecord( **r ) for r in
                       self.__db[self.__ts_log_prefix + str( _id )].find({"time": {"$gt": start_s,
                                                                                   "$lt": end_s}} )]
        except Exception, e:
            raise Exception("Query failed, %s" % str( e ))
        return records

    def current_value( self, metric_name=None, host_name=None ):
        if not isinstance( metric_name, basestring ) or not isinstance( host_name, basestring ):
            return None
        match = "[{$match: {$and: [{N: \"%s\"}, {H: \"%s\"}]}}, {$sort: {T: -1}}, {$limit: 1}]" % \
                (metric_name, host_name)
        return self.find( match )

    def percentiles( self, metric_name=None, host_names=None, time_s=None, dmax=(15 * 60)):
        if not isinstance( metric_name, basestring ) or not isinstance( host_names, list ):
            return None
        if not time_s:
            time_s = time.time()

        pipeline = """[
            {$match: {$and : [ {N: "%s"}, {H: {$in: %s}}, {T: {$gt: %d}}, {T: {$lte: %d}}]}},
            {$sort: {T: 1}},
            {$group: {_id: "$H", value: {$last: "$V"}}},
            {$project: {_id: 0, value: 1}}
        ]""" % (metric_name, str( host_names ), time_s - dmax, time_s)

        finalize = """
                function(v) {
                    Q = 10;
                    // sort values, calculate quantiles, return array of quantiles
                    v = v.sort(function(a, b){return a.value-b.value});
                    var quant = [];
                    for (var q=1; q <= Q; q++) {
                        var p = Math.round(q / Q * v.length);
                        quant[q-1] = v[p-1].value;
                    };
                    return quant;
                };
        """
        return self.aggregate( pipeline, finalize )

    def find( self, match=None ):
        return self.aggregate( match )

    def aggregate( self, pipeline=None, finalize="0" ):
        if not isinstance( pipeline, basestring ):
            return None
        self.__db.system_js.agg = """
            function(pipeline, finalize) {
                // select relevant collections and apply pipeline operation, concatenate results
                var vals = [];
                var cols = db.getCollectionNames();
                for (i=0; i < cols.length; i++) {
                    if (cols[i].indexOf("system.") < 0) {
                        vals = vals.concat(db[cols[i]].aggregate(pipeline).result);
                    };
                };
                // run finalize function
                if (finalize instanceof Function) {
                    return finalize(vals);
                } else {
                    return vals;
                };
            };
        """
        return self.__db.system_js.agg( Code( pipeline ), Code( finalize ) )
