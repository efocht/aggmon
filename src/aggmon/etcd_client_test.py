from etcd_client import EtcdQueueEmpty, EtcdTimeout, EtcdClient
from etcd_config import DEFAULT_CONFIG
import pprint


def comp(a, b):
    if isinstance(a, dict) and isinstance(b, dict):
        keys_a = sorted(a.keys())
        keys_b = sorted(b.keys())
        for key_a, key_b in zip(keys_a, keys_b):
            comp(a[key_a], b[key_b])
    else:
        if a != b:
            print a, "!=", b

if __name__ == "__main__":
    client = EtcdClient()
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(DEFAULT_CONFIG)
    client.serialize("/config", DEFAULT_CONFIG)
    config = client.deserialize("/config")
    print "---"
    pp.pprint(config)
    if cmp(DEFAULT_CONFIG, config) != 0:
        print "DEFAULT_CONFIG != config"
    comp(DEFAULT_CONFIG, config)

