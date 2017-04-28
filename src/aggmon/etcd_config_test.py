from etcd_config import *
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
    config = Config(config=DEFAULT_CONFIG, config_dir="../../config.d")
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(config.get("/"))

