#
# a little cache class, that simply puts things in a dict.
#

class MCache(object):
    def __init__(self):
        self.cache = {}

    def has_key(self, *args):
        key = "_".join(args)
        return key in self.cache

    def get(self, *args):
        key = "_".join(args)
        if key in self.cache:
            return self.cache[key]
        return None

    def set(self, *args):
        # the value is the last argument in the list
        args = list(args)
        val = args.pop(-1)
        key = "_".join(args)
        self.cache[key] = val

