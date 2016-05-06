import logging
import re


log = logging.getLogger( __name__ )


class MsgTagger(object):
    """
    Tag messages by adding a key value pair into the msg dict.

    A tag condition is described by match conditions which are represented by key-value
    pairs. The key must match a key in the message. If the value matches as well, the
    message gets the additional key-value of the tag. If the value string starts with
    "RE:" then the rest of the string is regarded as regular expression match string.
    The match mechanism is similar to that in the publisher match.

    The tags are stored in a dict. The key is the tag key. The value is a dict with two
    keys: "val" holds the tag value, "match" is an array of matches or compiler regexps.
    """
    def __init__(self, tags={}):
        self.tags = tags

    def add_tag(self, msg, *args, **kwds):
        log.info("add_tag: msg=%r" % msg)
        if "TAG_KEY" not in msg:
            raise Exception("No TAG_KEY in add_tag command message!")
        tag_key = msg["TAG_KEY"]
        del msg["TAG_KEY"]
        if "TAG_VALUE" not in msg:
            raise Exception("No TAG_VALUE in add_tag command message!")
        tag_value = msg["TAG_VALUE"]
        del msg["TAG_VALUE"]
        for k, v in msg.items():
            if isinstance(v, basestring):
                if v.startswith("RE:"):
                    matchre = "RE:".join(v.split("RE:")[1:])
                    comp = re.compile(matchre)
                    msg[k] = {"s": v, "c": comp}
                else:
                    # exact match
                    msg[k] = {"s": v}

        tkey = tag_key + ":" + tag_value
        self.tags[tkey] = {"key": tag_key, "val": tag_value, "match": [msg]}
        log.info( "Added tag '%s' : %r" % (tkey, msg) )
        return True

    def remove_tag(self, msg, *args, **kwds):
        log.info("remove_tag: msg=%r" % msg)
        if "TAG_KEY" not in msg:
            raise Exception("No TAG_KEY in add_tag command message!")
        tag_key = msg["TAG_KEY"]
        if "TAG_VALUE" not in msg:
            raise Exception("No TAG_VALUE in add_tag command message!")
        tag_value = msg["TAG_VALUE"]
        del msg["TAG_VALUE"]
        tkey = tag_key + ":" + tag_value
        if tkey in self.tags:
            del self.tags[tkey]
            log.info( "Removed tag '%s'" % tkey )

    def show_tags(self, msg, *args, **kwds):
        return self.tags

    def reset_tags(self, msg, *args, **kwds):
        self.tags = {}
        return True

    def do_tag(self, msg):
        """
        Tag the messages that match the tag conditions.
        """
        for tag, tv in self.tags.items():
            for match_dict in self.tags[tag]["match"]:
                # matches is True by default for the case that someone subscribed to all messages
                # which means: no match keys associated with a target.
                #pdb.set_trace()
                matches = True
                for mk, mv in match_dict.items():
                    if mk not in msg:
                        matches = False
                        break
                    v = msg[mk]
                    if "c" in mv:
                        if mv["c"].match(v) is None:
                            matches = False
                            break
                    elif mv["s"] != v:
                        matches = False
                        break
                if matches:
                    msg[tv["key"]] = tv["val"]
        return msg


