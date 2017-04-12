from etcd import *
from urllib3.exceptions import TimeoutError
import json


log = logging.getLogger( __name__ )


class EtcdQueueEmpty(EtcdException):
    pass

class EtcdTimeout(EtcdException):
    pass

class EtcdClient(Client):
    def __init__(self, **kwds):
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
            result = json.loads(reply.value)
        return result

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
