# -*- coding: utf-8 -*-
import os, os.path
import sys
import glob
from setuptools import setup, Extension

CONFIGDIR="/etc/aggmon"
PREFIX="/usr/local"
NAME="AggMon"
VERSION="0.2"
PYVER="%d.%d" % (sys.version_info[0],sys.version_info[1],)
ARCH=os.uname()[4]

quantiles = Extension("aggmon.quantiles",
                             include_dirs=[os.getcwd()+"/src/aggmon/module-quantiles/", "/usr/include/boost","."],
                             libraries=["boost_python", "dl"],
                             library_dirs=["/usr/lib/x86_64-linux-gnu/"],
                             sources=["src/aggmon/module-quantiles/MathUtils.cpp",
                                      "src/aggmon/module-quantiles/PyQuantiles.cpp",
                                      "src/aggmon/module-quantiles/Quantiles.cpp"])


if len(sys.argv) > 2 and sys.argv[1].startswith("install") and sys.argv[2].startswith("--prefix"):
    if "=" in sys.argv[2]:
        PREFIX = sys.argv[2].split("=")[1]
    elif len(sys.argv) > 3 and not sys.argv[3].startswith("-"):
        PREFIX = sys.argv[3]
    else:
        print "Cannot extract prefix from cmdline"

if len(sys.argv) > 1 and sys.argv[1] == "uninstall":
    flist = []
    dlist = []
    pypath = "%s/lib/python%s/dist-packages/AggMon-%s-py%s-linux-%s.egg" % (PREFIX,
                                                                            PYVER,
                                                                            VERSION,
                                                                            PYVER,
                                                                            ARCH,)
    flist += glob.glob(PREFIX+"/sbin/agg_*")
    flist += glob.glob(PREFIX+"/sbin/gmond_wrapper")
    flist += glob.glob(pypath+"/aggmon/*.py*")
    flist += glob.glob(pypath+"/aggmon/quantiles.so")
    dlist += [pypath+"/aggmon"]
    flist += glob.glob(pypath+"/EGG-INFO/*")
    dlist += [pypath+"/EGG-INFO"]
    flist += glob.glob(pypath+"/metric_store/*.py*")
    dlist += [pypath+"/metric_store"]
    flist += glob.glob(pypath+"/res_mngr/*.py*")
    dlist += [pypath+"/res_mngr"]
    dlist += [pypath]
    flist +=  ["/etc/init.d/aggmon"]
    flist +=  ["/etc/systemd/system/aggmon.service"]
    if len(sys.argv) == 3 and sys.argv[2] == "--all":
        flist += glob.glob(CONFIGDIR+"/*.yaml")
        dlist += [CONFIGDIR]

    try:
        for f in flist:
            if os.path.exists(f):
                os.remove(f)
        for d in dlist:
            if os.path.exists(d) and len(glob.glob(d+"/*")) == 0:
                os.rmdir(d)
    except Exception, e:
        print e
        sys.exit(1)
    
    sys.exit(0)

setup(
    name = NAME,
    version = VERSION,
    maintainer = "Erich Focht",
    maintainer_email = "erich.Focht@EMEA.NEC.COM",
    author = u"Thomas Röhl",
    author_email = "Thomas.Roehl@fau.de",
    description = ("A general purpose monitoring and aggregation infrastructure."),
    license = "GPLv2",
    keywords = "monitoring",
    url = "https://github.com/efocht/aggmon",
    long_description = ("A general purpose monitoring and aggregation infrastructure."),
    classifiers=[
        "Development Status :: 1 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: GPL License",
    ],
    install_requires=[
        'PyYAML',
        'ujson',
        'pymongo',
        'pyzmq',
    ],
    package_data = {
        "aggmon" : ["src/aggmon/packet_sender.py"],
        "aggmon.quantiles" : ["src/aggmon/basic_aggregators.py", quantiles],
        "aggmon.agg_cmd" : ["src/aggmon/agg_cmd.py"],
        "aggmon.agg_collector" : ["src/aggmon/agg_collector.py"],
        "aggmon.agg_control" : ["src/aggmon/agg_control.py"],
        "aggmon.data_store" : ["src/aggmon/data_store.py"],
        "aggmon.agg_job_agg" : ["src/aggmon/agg_job_agg.py"],
        "aggmon.agg_job_command" : ["agg_job_command.py"],
        "aggmon.agg_rpc" : ["src/aggmon/agg_rpc.py"],
        "aggmon.agg_component" : ["src/aggmon/agg_component.py"],
        "aggmon.repeat_timer" : ["src/aggmon/repeat_timer.py"],
        "aggmon.scheduler" : ["src/aggmon/scheduler.py"],
        "aggmon.agg_mcache" : ["src/aggmon/agg_mcache.py"],
        "aggmon.msg_tagger" : ["src/aggmon/msg_tagger.py"],
        "aggmon.repeat_event" : ["src/aggmon/repeat_event.py"],
        "aggmon.basic_aggregators" : ["src/aggmon/basic_aggregators.py"],
        "metric_store.metric_store" : ["src/metric_store/metric_store.py"],
        "metric_store.mongodb_store" : ["src/metric_store/mongodb_store.py"],
        "metric_store.influxdb_store" : ["src/metric_store/influxdb_store.py"],
        "res_mngr.pbsnodes" : ["src/res_mngr/pbsnodes.py"]
    },
    package_dir = {"aggmon" : "src/aggmon",
                   "metric_store" : "src/metric_store",
                   "res_mngr" : "src/res_mngr"},
    ext_modules = [quantiles],
    py_modules = ["aggmon.agg_cmd", "aggmon.agg_collector", "aggmon.agg_control",
                  "aggmon.data_store", "aggmon.agg_job_agg", "aggmon.agg_rpc",
                  "aggmon.agg_component", "aggmon.agg_job_command", "aggmon.scheduler",
                  "aggmon.repeat_timer", "aggmon.repeat_event", "aggmon.msg_tagger",
                  "aggmon.agg_mcache", "aggmon.basic_aggregators", "aggmon.quantiles",
                  "metric_store.metric_store", "metric_store.mongodb_store",
                  "metric_store.influxdb_store", "res_mngr.pbsnodes"],
    data_files = [("/etc/aggmon", ["config.d/config.yaml", "config.d/aggregate.yaml", "config.d/agg-templates.yaml"]),
                  #("/etc/init.d", ["misc/aggmon"]),
                  ("/etc/systemd/system", ["aggmon.service"]),
                  ("/usr/local/sbin", ["bin/agg_cmd", "bin/agg_collector", "bin/agg_control", "bin/agg_datastore", "bin/agg_jobagg", "bin/gmond_wrapper"])],
)


