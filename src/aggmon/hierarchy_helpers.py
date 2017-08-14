#
# Helper functions to navigate through configured hierarchies.
#


def hierarchy_from_url(url):
    """
    Decode hierarchy and hierarchy_key from a hierarchy URL.

    The URL has the format:
    <hierarchy_name>:<hierarchy_path>
    for example:
    monitor:/universe/rack2

    This function returns a tuple made of the hierarchy name and a flattened "hierarchy_key",
    for example: ("universe", "universe_rack2")
    The hierarchy_key can be used as a shard key in the data stores.
    """
    hierarchy = None
    key = None
    path = None
    if url.find(":") > 0:
        hierarchy = url[: url.find(":")]
        path = url[url.find(":") + 1 :]
        key = "_".join(path.split("/")[1:])
    return hierarchy, key, path

def parent_hierarchy_url(hierarchy_url):
    hname, hkey, hpath = hierarchy_from_url(hierarchy_url)
    if hname is None:
        return None
    parent_hpath = "/".join(hpath.split("/")[:-1])
    if not parent_hpath:
        parent_hpath = hpath
    return "%s:%s" % (hname, parent_hpath)

def top_hierarchy_url(hierarchy_url):
    hname, hkey, hpath = hierarchy_from_url(hierarchy_url)
    if hname is None:
        return None
    top_hpath = "/".join(hpath.split("/")[:2])
    return "%s:%s" % (hname, top_hpath)

def top_config_hierarchy_url(config, htype):
    if htype == "group":
        hierarchy = config.get("/hierarchy/%s" % htype)
        hkey = hierarchy.keys()[0]
        return top_hierarchy_url(hierarchy[hkey]["hpath"])
    else:
        return None
