import time
import datetime
from metric_store.mongodb_store import *
from bson.objectid import ObjectId
import pdb

# delete all collections in MongoDBMetricStore
MongoDBMetricStore( db_name="metric_test" ).drop_all()

#pdb.set_trace()
store = MongoDBMetricStore( db_name="metric_test" )

# add metric
epoche = int( time.time() )
store.insert_val( {"N": "load_one", "H": "localhost", "V": 2.71, "T": epoche} )
store.insert_val( {"N": "load_one", "H": "localhost", "V": 3.14, "T": epoche} )
for r in store.find_val():
    print r
store.close()

# create another MongoDBMetricStore object (this should NOT create a new partition)
store = MongoDBMetricStore( db_name="metric_test" )
epoche = int( time.time() )
store.insert_val( {"N": "load_one", "H": "localhost", "V": 5.08, "T": epoche} )
store.insert_val( {"N": "load_one", "H": "localhost", "V": 6.66, "T": epoche} )

# wait and create a new partition if current partition is older than 4 seconds
time.sleep( 5 )
epoche = int( time.time() )
store.update_partitions( partition_timeframe=4 )
store.insert_val( {"N": "load_one", "H": "localhost", "V": 7.07, "T": epoche} )
store.insert_val( {"N": "load_one", "H": "localhost", "V": 8.00, "T": epoche} )

# force creation of a new partition
epoche = int( time.time() )
store.update_partitions( partition_timeframe=0 )
store.insert_val( {"N": "load_one", "H": "localhost", "V": 9.81, "T": epoche} )
store.insert_val( {"N": "load_one", "H": "localhost", "V": 0.33, "T": epoche} )

for r in store.find_val():
    print r

