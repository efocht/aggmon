import time
import datetime
from metric_store.mongodb_store import *
from bson.objectid import ObjectId
import pdb

# delete all collections in MongoDBMetricStore
MongoDBMetricStore( db_name="metric_test" ).drop_all()

#pdb.set_trace()
store = MongoDBMetricStore( db_name="metric_test" )

# add metric that is 50 days old
epoch = int( time.time() ) - 60 * 60 * 24 * 50
gen_time = datetime.datetime.utcfromtimestamp( epoch )
store.insert_val( {"_id": ObjectId.from_datetime( gen_time ), "N": "load_one", "H": "localhost", "V": 3.14, "T": epoch} )

# add metric with current time
epoche = int( time.time() )
store.insert_val( {"N": "load_one", "H": "localhost", "V": 2.71, "T": epoche} )
for r in store.find_val():
    print r
store.close()

# create another MongoDBMetricStore object (this should NOT create a new partition)
store = MongoDBMetricStore( db_name="metric_test" )
epoche += 1
store.insert_val( {"N": "load_one", "H": "localhost", "V": 5.08, "T": epoche} )

# at this point there should be
#    one partition with two records and
#    a second partition with one entry
for r in store.find_val():
    print r

