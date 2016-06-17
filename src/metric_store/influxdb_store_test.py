import sys
import os
import time
import datetime
import pdb

# Path Fix
sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), "../aggmon")))

from influxdb_store import InfluxDBMetricStore

#pdb.set_trace()
store = InfluxDBMetricStore()

# add metric
epoche = int( time.time() )
store.insert( {"NAME": "load_one", "HOST": "localhost", "VALUE": 2.71, "TIME": epoche} )
store.insert( {"NAME": "load_one", "HOST": "localhost", "VALUE": 3.14, "TIME": epoche + 1} )
store.insert( {"NAME": "load_one", "HOST": "localhost", "VALUE": 9.81, "TIME": epoche + 2} )
# wait until batch times out and writes data to db
time.sleep( 2 )
store.insert( {"NAME": "load_one", "HOST": "localhost", "VALUE": 1.23, "TIME": epoche + 3} )
records = store.find( "SELECT * FROM load_one" )
for r in records:
    if "series" in r:
        for s in r["series"]:
            print s["name"], s["columns"]
            for v in s["values"]:
                print v
    else:
        print r

