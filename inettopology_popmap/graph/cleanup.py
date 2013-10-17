
import inettopology_popmap.connection as connection


def cleanup(args):
    r = connection.Redis()
    igraph_keys = r.keys('graph:*')
    for key in igraph_keys:
        r.delete(key)
