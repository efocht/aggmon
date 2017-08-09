from etcd_client import EtcdQueueEmpty, EtcdTimeout, EtcdClient
from agg_config import DEFAULT_CONFIG
import pprint
import sys

def comp(a, b):
    if isinstance(a, dict) and isinstance(b, dict):
        keys_a = sorted(a.keys())
        keys_b = sorted(b.keys())
        for key_a, key_b in zip(keys_a, keys_b):
            comp(a[key_a], b[key_b])
    else:
        if a != b:
            print a, "!=", b

def bucket(selector, buckets={}):
    """
    This function returns always the same key out of the bucket dict
    as long as the key's value equals 1. The selection is based on
    selector.
    buckets = {"h3": 0, "h1": 1, "h4": 1, "h2": 1}
    """
    import mmh3
    sorted_names = [name for name, state in sorted(buckets.items(), key=lambda(name, state): name)]
    active_names = [name for name in sorted_names if buckets[name] == 1]
    total = len(buckets)
    active = len(active_names)
    for n in range(total, 1, -1):
        bucket_number = mmh3.hash(selector) % n
        if bucket_number < active:
            return active_names[bucket_number]
    return False

if __name__ == "__main__":
#    hosts = {"h3": 0, "h1": 1, "h4": 1, "h2": 1}
#    for x in range(20):
#        print bucket("h3" + str(x*5), hosts)
#    sys.exit(0)
#
    client = EtcdClient()
    print "machines in etcd cluster: %s" % str(client.machines)
    print "etcd cluster leader: %s" % str(client.leader)
    pp = pprint.PrettyPrinter(indent=4)
    #pp.pprint(DEFAULT_CONFIG)
    print "store config object in etcd..."
    try:
        client.serialize("/config", DEFAULT_CONFIG)
    except:
        client.update("/config", DEFAULT_CONFIG)
    print "retrieve config object from etcd..."
    config = client.deserialize("/config")
    sys.stdout.write("compare...")
    #pp.pprint(config)
    if cmp(DEFAULT_CONFIG, config) != 0:
        print " DEFAULT_CONFIG != config"
        comp(DEFAULT_CONFIG, config)
    else:
        print " ok"
#    print "watch /config recusively for changes (max 30 sec)..."
#    res = client.watch("/config", timeout=30, recursive=True)
#    print type(res), repr(res)

    print "delete /config..."
    client.delete("/config",  recursive=True)

