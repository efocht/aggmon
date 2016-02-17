#
# Parse pbsnodes output and return a dict of its content.
#

import logging
import os
import pdb
import subprocess
import re
import sys
import time
# Path Fix
sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.dirname(
                os.path.realpath(__file__)), "../")))
from metric_store.mongodb_store import *


log = logging.getLogger( __name__ )

test_input = """

e1165
     state = free
     np = 40
     properties = turbo,noturbo,f2.2,f2.1,f2.0,f1.9,f1.8,f1.7,f1.6,f1.5,f1.4,f1.3,f1.2,ddr1866,ddrany,likwid,eswge12,rack11,hdd,k20m,k20m2x
     ntype = cluster
     status = rectime=1455702949,varattr=,jobs=,state=free,netload=1285709082744,gres=,loadave=0.00,ncpus=40,physmem=66071900kb,availmem=75062044kb,totmem=76071256kb,idletime=151393,nusers=0,nsessions=0,uname=Linux e1165 2.6.32-573.8.1.el6.x86_64 #1 SMP Tue Nov 10 18:01:38 UTC 2015 x86_64,opsys=linux
     mom_service_port = 15002
     mom_manager_port = 15003
     gpu_status = timestamp=Wed Feb 17 10:55:49 2016

e1166
     state = job-exclusive
     np = 40
     properties = turbo,noturbo,f2.2,f2.1,f2.0,f1.9,f1.8,f1.7,f1.6,f1.5,f1.4,f1.3,f1.2,ddr1866,ddrany,likwid,eswge12,rack11,hdd,k20m,k20m2x
     ntype = cluster
     jobs = 0/535654.eadm,1/535654.eadm,2/535654.eadm,3/535654.eadm,4/535654.eadm,5/535654.eadm,6/535654.eadm,7/535654.eadm,8/535654.eadm,9/535654.eadm,10/535654.eadm,11/535654.eadm,12/535654.eadm,13/535654.eadm,14/535654.eadm,15/535654.eadm,16/535654.eadm,17/535654.eadm,18/535654.eadm,19/535654.eadm,20/535654.eadm,21/535654.eadm,22/535654.eadm,23/535654.eadm,24/535654.eadm,25/535654.eadm,26/535654.eadm,27/535654.eadm,28/535654.eadm,29/535654.eadm,30/535654.eadm,31/535654.eadm,32/535654.eadm,33/535654.eadm,34/535654.eadm,35/535654.eadm,36/535654.eadm,37/535654.eadm,38/535654.eadm,39/535654.eadm
     status = rectime=1455702940,varattr=,jobs=535654.eadm,state=free,netload=1469904411590,gres=,loadave=0.17,ncpus=40,physmem=66071900kb,availmem=75077452kb,totmem=76071256kb,idletime=73585,nusers=0,nsessions=0,uname=Linux e1166 2.6.32-573.8.1.el6.x86_64 #1 SMP Tue Nov 10 18:01:38 UTC 2015 x86_64,opsys=linux
     mom_service_port = 15002
     mom_manager_port = 15003
     gpu_status = timestamp=Wed Feb 17 10:55:40 2016
"""

    

def check_output(*popenargs, **kwargs):
    process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
    output, unused_err = process.communicate()
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        error = subprocess.CalledProcessError(retcode, cmd)
        error.output = output
        raise error
    return output

# trick for adding check_output on python < 2.7
try:
    subprocess.check_output
except:
    subprocess.check_output = check_output


def conv_from_str(s):
    try:
        s = int(s)
    except ValueError:
        try:
            s = float(s)
        except ValueError:
            pass
    return s


class PBSNodes(object):
    def __init__(self, host="localhost", port=22, user=""):
        self._host = host
        self._port = port
        self._user = user
        if host != "localhost":
            self._cmd = "ssh -p %d %s pbsnodes" % (self._port, len(user) > 0 and "%s@%s" % (self._user, self._host) or self._host)
        else:
            self._cmd = "pbsnodes"
        self._last_update = 0
        self.state = {}
        self.job_nodes = {}

    @staticmethod
    def parse(inp):
        state = {}
        node = None
        for line in inp.split("\n"):
            # node block start
            m = re.match("^(\S+)$", line)
            if m:
                node = m.group(1)
                state[node] = {}
                continue
            # node attribute line
            m = re.match("^\s+(\S+) = (.*)$", line)
            if m:
                if node is None:
                    log.error("Unexpected node attribute line. No node set, yet! Skipping.")
                    continue
                attrib = m.group(1)
                value = m.group(2)
                m = re.match("^\S+=\S+", value)
                if m:
                    # sub-attributes match
                    subattribs = {}
                    for elem in value.split(","):
                        k, v = elem.split("=")
                        subattribs[k] = conv_from_str(v)
                    state[node][attrib] = subattribs
                    continue
                m = re.match("^[^=]+,", value)
                if m:
                    # list match
                    state[node][attrib] = value.split(",")
                    continue
                # no sub-attributes
                state[node][attrib] = conv_from_str(value)
        return state

    def update(self, test=False):
        try:
            if not test:
                out = subprocess.check_output(self._cmd, stderr=subprocess.STDOUT, shell=True)
            else:
                out = test_input
        except Exception as e:
            log.error("subprocess error when running '%s' : '%r'" % (self._cmd, e))
        else:
            self.state = self.parse(out)
            self._last_update = time.time()
            self.update_job_nodes()

    def update_job_nodes(self):
        jobs = {}
        for node, attr in self.state.items():
            jobids = attr["status"]["jobs"].split(" ")
            jobids = [ v.replace(".", "_") for v in jobids if len(v) > 0 ]
            for jobid in jobids:
                if jobid in jobs:
                    jobs[jobid].append(node)
                else:
                    jobs[jobid] = [node]
        self.job_nodes = jobs


def mongodb_joblist():
    dbconf = {
        "dbname": "metricdb",
        "jobdbname": "metric",
        "dbhost": "localhost:27017",
        "user": "",
        "password": ""
    }

    store = MongoDBJobList(host_name=dbconf["dbhost"], port=None, db_name=dbconf["jobdbname"],
                           username=dbconf["user"], password=dbconf["password"])
    job_list = {}
    for j in store.find():
        jobid = j["name"]
        if "cnodes" in j:
            job_list[jobid] = j["cnodes"]
        else:
            log.warning("loading job_list: skipping job without nodes! jobid=%s" % jobid)
    return job_list


if __name__ == "__main__":
    from aggmon.agg_rpc import send_rpc
    import zmq

    host = "localhost"; port = 22
    if len(sys.argv) > 1:
        host = sys.argv[1]
    if len(sys.argv) > 2:
        port = int(sys.argv[2])

    pbs = PBSNodes(host=host, port=port)
    pbs.update()
    mjoblist = mongodb_joblist()

    num_jobs_mongo = len(mjoblist.keys())
    num_jobs_pbs = len(pbs.job_nodes.keys())
    
    print "PBSNodes sees %d jobs" % len(pbs.job_nodes.keys())
    print "MongoDB  sees %d jobs" % len(mjoblist.keys())

    print "PBSNodes - MongoDB = %r" % list(set(pbs.job_nodes.keys()) - set(mjoblist.keys()))
    print "MongoDB - PBSNodes = %r" % list(set(mjoblist.keys()) - set(pbs.job_nodes.keys()))

    zmq_context = zmq.Context()
    tags = send_rpc(zmq_context, "tcp://10.28.8.21:5558", "show_tags")
    coll = tags.keys()[0]
    tags = tags[coll]
    tags_jobs = [ j.lstrip("J:") for j in tags.keys() ]
    print "Tags set for %d jobs" % len(tags_jobs)

    print "PBSNodes - Tags = %r" % list(set(pbs.job_nodes.keys()) - set(tags_jobs))
    print "MongoDB - Tags  = %r" % list(set(mjoblist.keys()) - set(tags_jobs))
    
    print "Tags - PBSNodes = %r" % list(set(tags_jobs) - set(pbs.job_nodes.keys()))
