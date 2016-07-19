"""
Send metrics to a [influxdb](https://github.com/influxdb/influxdb/) using http

Patched by Thomas Roehl (Thomas.Roehl@fau.de) for the FEPA project to add
collector and metric tags to the json.
"""
import time
import logging
import math
import os, os.path
import re, subprocess
import json
import subprocess
from metric_store import MetricStore

__all__ = ["InfluxDBMetricStore"]

BATCH_SIZE = 96
CACHE_SIZE = 1024
TIME_PRECISION = "s"
TAGFILE = ""

log = logging.getLogger( __name__ )

class InfluxDBStore(object):
    def __init__( self, hostname="localhost", port=None, db_name="metric", username="root", password="root" ):
        self.hostname = hostname
        self.db_name = db_name
        self.username = username
        self.password = password
        self.port = 8086
        self.url="http://"
        if port and ( isinstance( port, int ) or isinstance( port, basestring ) ):
                self.port = int(port)
        if self.port == 8084:
            self.url="https://"
        if self.username and self.password:
            self.url += "%s:%s@" % (self.username, self.password)
        elif self.username:
            self.url += "%s@" % self.username
        self.url += "%s:%s" % (self.hostname, self.port)
        self.curl_get = "curl --get -is %s/query" % self.url
        self.curl_write = "curl -is %s/write" % self.url

    def create_db( self, ext_name="" ):
        """
        curl --get -i http://localhost:8086/query --data-urlencode "q=CREATE DATABASE metric_universe"
        """
        sql_cmd = "'q=CREATE DATABASE %s'" % (self.db_name + ext_name)
	curl_cmd = self.curl_get + " --data-urlencode " + sql_cmd
        return self.exec_cmd( curl_cmd )

    def query( self, query, ext_name="" ):
        """
        curl --get -i http://localhost:8086/query? \
                                     --data-urlencode 'db=metric-universe' \
                                     --data-urlencode 'q=SELECT value FROM load_one'
        """
        sql_cmd = "q=%s" % query
	curl_cmd = self.curl_get + "?" + " --data-urlencode 'db=" + self.db_name + ext_name + "' --data-urlencode '" + sql_cmd + "'"
        return self.exec_cmd( curl_cmd )

    def write( self, data, ext_name="" ):
        """
        curl -i http://localhost:8086/write?db=metric_universe --data-binary 'cpu_load_short,host=server01,region=us-west value=0.64 1434055562000000000'
        """
	curl_cmd = self.curl_write + "?db=" + self.db_name + ext_name + " --data-binary "

        sendlist = []
        for m in data:
            tags = [] 
            for k in m["tags"].keys():
                tags.append("%s=%s" % (str(k),str(m["tags"][k]),))
            mstr = m["measurement"]
            if len(tags) > 0:
                mstr += ","+",".join(tags)
            mstr += " value=%s %s" % (str(m["fields"]["value"]),str(int(m["time"]*1E9)),) 
            sendlist.append(mstr)

        curl_cmd = curl_cmd + " '" + "\n".join(sendlist) + "'"
        return self.exec_cmd( curl_cmd )

    def drop_all( self, ext_name="" ):
        """
        Drops all content; not the database itself.
        curl -i http://localhost:8086/query --data-urlencode "q=DROP DATABASE IF EXISTS metric_universe;CREATE DATABASE metric_universe"
        """
        db_name = self.db_name + ext_name
        sql_cmd = "'q=DROP DATABASE IF EXISTS %s;CREATE DATABASE %s'" % (db_name, db_name)
	curl_cmd = self.curl_get + " --data-urlencode " + sql_cmd
        return self.exec_cmd( curl_cmd )

    @staticmethod
    def exec_cmd( cmd ):
        proc = subprocess.Popen( cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE )
        o, e = proc.communicate() #return (stdout, stdierr)
        p = "### send:\n" + cmd + "\n"
        p += "### stdout:\n" + o if o else ""
        p += "### stderr:\n" + e if e else ""
        p += "\n"
        log.debug(p)
        return o, e


class InfluxDBMetricStore(InfluxDBStore, MetricStore):
    """
    Numerical (Ganglia style) metrics
    Send to InfluxDB using batched HTTP
    """
    def __init__(self, hostname="localhost", port=None, db_name="metric", username="root", password="root", group="/universe",
                 md_col="md", val_col="", val_ttl=3600*24*180, **kwds):
        InfluxDBStore.__init__( self, hostname=hostname, port=port, db_name=db_name, username=username, password=password )
        self.group = group
        self.create_db( "_" + MetricStore.group_suffix( self.group ) )
        self._val_ttl = val_ttl
        self.batch_size = BATCH_SIZE
        self.metric_max_cache = CACHE_SIZE
        self.batch_count = 0
        self.time_precision = TIME_PRECISION
        self.addtags = False
        if TAGFILE:
            self.tagfile = TAGFILE
            self.addtags = True
        self.batch = {}
        self.batch_timestamp = time.time()
        self.time_multiplier = 1

    def find(self, match=""):
        o, _e = self.query( match, "_" + MetricStore.group_suffix( self.group ) )
        results = []
        try:
            results = json.loads( o );
            return results["results"]
        except:
            return results

    def get_md(self):
        return None

    def insert(self, metric):
        """
        Add metric to batch, send batch if sufficient data is available or batch timed out
        """
        path = self.to_path( metric )
        metric["path"] = path
        if self.batch_count <= self.metric_max_cache:
            if not self.batch.has_key(path):
                self.batch[path] = []
                #log.error("New key in batch: %s" % str(path))
            t = metric["TIME"] if "TIME" in metric else metric["T"]
            v = metric["VALUE"] if "VALUE" in metric else metric["V"]
            self.batch[path].append([t, v])
            self.batch_count += 1
        if self.batch_count >= self.batch_size or (time.time() - self.batch_timestamp) > 2**self.time_multiplier:
            self.batch_timestamp = time.time()
            self.send_batch()

    def send_batch(self):
        """
        Send data to Influxdb. Data that can not be sent will be kept in queued.
        """
        metrics = []
        def append_metric(time, tags, mname, value):
            try:
                value = float(value)
                if math.isinf(value) or math.isnan(value):
                    value = 0
            except:
                value = 0
            mjson = {"time": time, "tags": tags, "measurement": mname, "fields": {"value": value}}
            metrics.append(mjson)
            log.debug("metric added: %s, %s, %s" % (str(tags), str(mname), str(value)))

        try:
            # build metrics data
            tags ={}
            for path in self.batch.keys():
                # ex. path: server.node6.likwid.cpu1.dpmflops
                pathlist = path.split(".")
                if len(pathlist) >= 4:
                    pathlist.pop(0)
                    mname = pathlist[-1]
                    pathlist.pop()
                    host = pathlist[0]
                    pathlist.pop(0)
                    collector = pathlist[0]
                    pathlist.pop(0)
                    tags = {"host": host, "collector" : collector}
                    for p in pathlist:
                        if p.startswith("cpu"):
                            tags["cpu"] = p.replace("cpu","")
                            pathlist[pathlist.index(p)] = ""
                        elif p.startswith("total"):
                            mname = "sum."+mname
                            pathlist[pathlist.index(p)] = ""
                    if collector == "likwid":
                        for p in pathlist:
                            if p in ["avg","min","max","sum"]:
                                mname = p+"."+mname
                                pathlist[pathlist.index(p)] = ""
                    elif collector == "iostat":
                        tags["disk"] = pathlist[0]
                        pathlist[0] = ""
                else:
                    mname = path
                for item in self.batch[path]:
                    time = item[0]
                    value = item[1]

                    if isinstance(value, list):
                        quants = value[0]
                        if isinstance(quants, list):
                            # tags = {'host': u'tb033', 'collector': u'likwid', 'cpu': u'6'}
                            # mname = dpmuops_quant10
                            # value = [[28.0601539612, 28.0601539612, 28.0601539612, 28.0601539612, 28.0601539612, 28.0601539612, 28.0601539612, 28.0601539612, 28.0601539612, 28.0601539612, 28.0601539612], 28.0601539612]
                            nquants = len(quants)
                            for n in xrange(0, nquants):
                                tags["quant"] = 100 / (nquants - 1) * n
                                append_metric(time, tags, mname, quants[n])
                            if len(value) >= 2:
                                tags["quant"] = "avg"
                                append_metric(time, tags, mname, value[1])
                    elif isinstance(value, basestring) or isinstance(value, float) or isinstance(value, int) or isinstance(value, long):
                        append_metric(time, tags, mname, value)
                    else:
                        log.warn("Don't know how to handle metric with value type %s. Metric ignored!" % type(value))
            ret = self.write(metrics, "_" + MetricStore.group_suffix( self.group ))
            if ret:
                self.batch = {}
                self.batch_count = 0
                self.time_multiplier = 1
        except Exception, e:
            raise e

    @staticmethod
    def to_path( metric ):
        """
        path is: <"servers"|"instance">.$hostname.$collectorname.$metricname
                 e.g. servers.host.cpu.total.idle
        """
        host = metric["HOST"] if "HOST" in metric else metric["H"]
        name = metric["NAME"] if "NAME" in metric else metric["N"]
        if name.split(".")[0] == "servers":
            return name
        return "servers." + host + "." + name

