import logging
try:
    from agg_mcache import MCache
except:
    from aggmon.agg_mcache import MCache

log = logging.getLogger( __name__ )

class MetricStore:
    def __init__(self):
        self.md_cache = MCache()
        self.v_cache = MCache()
        self.load_md_cache()

    def load_md_cache(self):
        md = self.get_md()
        if not md:
            log.warn("md cache is empty!")
            return
        for d in md:
            log.debug("md cache data found: HOST=%s, NAME=%s, TYPE=%s" % (str(d["HOST"]), str(d["NAME"]), str(d["TYPE"])))
            self.md_cache.set(d["HOST"], d["NAME"], d["TYPE"])

    def show_md_cache(self):
        for k, v in self.md_cache.items():
            print("%s: %s" % (k, v))
 
    def insert(self, val):
        raise NotImplementedError("Must implement method in derived class!")
 
    def get_md(self):
        raise NotImplementedError("Must implement method in derived class!")

    @staticmethod
    def group_suffix( group ):
        suffix = group.lstrip("/").replace("/", "_")
        return suffix

