from etcd import *
from urllib3.exceptions import TimeoutError
import json
import os


log = logging.getLogger( __name__ )


class EtcdQueueEmpty(EtcdException):
    pass

class EtcdTimeout(EtcdException):
    pass

class EtcdClient(Client):
    def __init__(self, **kwds):
        # for test purposes: set host and port
        try:
            kwds["host"] = os.environ["ETCDHOST"]
            kwds["port"] = int(os.environ["ETCDPORT"])
        except:
            pass
        super(EtcdClient, self).__init__(**kwds)

    def deserialize(self, path):
        """
        Return directory/key-value hierarchy as a hierarchy of dicts.
        """
        result = None
        reply = super(EtcdClient, self).read(path)
        if reply.dir:
            result = dict()
            for child in reply.leaves:
                if child.key == path:
                    continue
                child_key = child.key.split("/")[-1]
                result[child_key] = self.deserialize(child.key)
        else:
            result = json.loads(str(reply.value))
        return result

    def serialize(self, obj=None, base_path=""):
        """
        Serialize a nested structure of dicts into an etcd directory tree.
        Objects of type dict within the config will be serialized as directories.
        All values are JSON encoded.
        """
        if isinstance(obj, dict):
            for key, value in obj.items():
                path = base_path + "/" + key
                self.serialize(value, path)
        else:
            etcd_file = json.dumps(obj)
            super(EtcdClient, self).write(base_path, etcd_file)

    def get(self, key):
        res = super(EtcdClient, self).get(key)
        val = None
        if not res.dir:
            val = json.loads(res.value)
        return val

    def set(self, key, val, ttl=None):
        json_val = json.dumps(val)
        return super(EtcdClient, self).set(key, json_val, ttl=ttl)

    def qput(self, qkey, val):
        """
        Put val into a queue using a sequential key.
        qkey specifies the key for the directory containing the queue.
        """
        val = json.dumps(val)
        return self.write(qkey, val, append=True)

    def qget(self, qkey, index=None, timeout=None, wait=False):
        """
        Pop and return an entry from queue in sequential order.

        A tuple of key and json deserialized value is returned, such that
        the consumer can use the key for posting a result into a completion queue.
        """
        try:
            #logger = logging.getLogger("etcd.client")
            #old_log_level = logger.getEffectiveLevel()
            #logger.setLevel(logging.CRITICAL)
            res = self.pop(self.read(qkey, sorted=True, wait=wait, recursive=True, dir=False,
                                     waitIndex=index, timeout=timeout).children.next().key)
            #logger.setLevel(old_log_level)
        except EtcdNotFile:
            raise EtcdQueueEmpty
        except TimeoutError:
            raise EtcTimeout
        except Exception as e:
            raise e
        return res.key, json.loads(res.value)


if __name__ == "__main__":
    import pprint
    from config import DEFAULT_CONFIG
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(DEFAULT_CONFIG)
    client.serialize(DEFAULT_CONFIG, "/config")
    config = client.deserialize("/config")
    pp.pprint(config)
    print cmp(DEFAULT_CONFIG, config)

