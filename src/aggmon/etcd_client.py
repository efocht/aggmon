# etcd client project on github:
#    https://github.com/jplana/python-etcd
from etcd import *
from urllib3.exceptions import TimeoutError
from etcd import EtcdException
from etcd import Client
import json
import logging
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
            if "host" in kwds and "port" not in kwds:
                kwds["port"] = 2379
        super(EtcdClient, self).__init__(**kwds)

    def deserialize(self, path):
        """
        Return directory/key-value hierarchy as a hierarchy of nested dicts.
        """
        result = None
        reply = super(EtcdClient, self).read(path)
        if reply.dir:
            result = dict()
            for child in reply.leaves:
                if child.key == path:
                    continue
                child_key = str(child.key.split("/")[-1])
                result[child_key] = self.deserialize(child.key)
        else:
            if not reply.value:
                reply.value = "{}"
            result = json.loads(reply.value)
        return result

    def serialize(self, base_path="", obj=None):
        """
        Serialize a nested structure of dicts into an etcd directory tree.
        Objects of type dict within the config will be serialized as directories.
        All values are JSON encoded.
        """
        if isinstance(obj, dict):
            super(EtcdClient, self).write(base_path, None, dir=True)
            for key, value in obj.items():
                path = base_path + "/" + key
                self.serialize(path, value)
        else:
            etcd_file = json.dumps(obj)
            super(EtcdClient, self).set(base_path, etcd_file)

    def update(self, path, new):
        """
        Update the etcd directory/key-value hierarchy at _path_ according to the
        object _new_ passed in as argument. Do this with minimal operations.
        New keys will be added, missing keys will be deleted, therefore the API
        is different from what a dict().update() does!
        """
        old = self.deserialize(path)

        if not isinstance(new, dict):
            if isinstance(old, dict):
                super(EtcdClient, self).delete(path, recursive=True, dir=True)
            if old != new:
                # new is not a dict: simply serialize it
                value = json.dumps(new)
                super(EtcdClient, self).write(path, value)
        else:
            if not isinstance(old, dict):
                super(EtcdClient, self).delete(path, recursive=True, dir=True)
            else:
                # - are any keys in old to be deleted?
                for key in set(old.keys()) - set(new.keys()):
                    super(EtcdClient, self).delete(path + "/" + key, recursive=True, dir=True)
                    del old[key]
                # - are there new keys?
                for key in set(new.keys()) - set(old.keys()):
                    self.serialize(path + "/" + key, new[key])
                    del new[key]
            for key, value in new.items():
                self.update(path + "/" + key, value)

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

    def keys(self, path, strip_parent=False, timeout=None):
        """
        Returns a list of the keys inside a path.
        """
        try:
            res = self.read(path, timeout=timeout)
        except TimeoutError:
            raise EtcTimeout
        except Exception as e:
            raise e
        if strip_parent:
            _lenpath = len(path)
            out = [c.key[_lenpath + 1:] for c in res.children]
        else:
            out = [c.key for c in res.children]
        return out

