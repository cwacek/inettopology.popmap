import logging
log = logging.getLogger(__name__)

import sys
import json
import time
import networkx as nx
import pkg_resources
import operator
import random
import multiprocessing

from inettopology.util.decorators import timeit
from inettopology.util.general import Color, ProgressTimer
import inettopology_popmap.connection as connection
import inettopology_popmap.data.dbkeys as dbkeys
from inettopology_popmap.graph.objects import (
    LinkDict, EdgeLink, VertexList, Stats)
import inettopology_popmap.graph.util as util
import inettopology_popmap.graph.objects as graph_objects
import inettopology_popmap.graph.datautil as datautil
import inettopology_popmap.graph.concurrent as concurrent
from inettopology_popmap.data.cleanup import pipelined_delete


def rand_key(keybase):
    return "%s:%s" % (keybase, random.randint(0, 10000))


SP_KEY = rand_key('core:sp_to_process')
TYPE_KEY = rand_key('core:types')
USED_KEY = rand_key('core:core_nodes')
PATH_KEY = rand_key('core:core_paths')
thread_graph = None
thread_r = None
log_out = None


def create_graph(args):

    r = connection.Redis()
    if args.xml:

        log.info("Loading saved graph from file: %s" % args.xml)

        @timeit
        def load_graph(path):
          return nx.Graph(nx.read_graphml(path))

        graph, time_taken = load_graph(args.xml)
        log.info("Graph loading complete in %0.2f seconds" % time_taken)
        graphpath = args.xml

    else:
        graph = load_from_redis(r, args)
        graphpath = args.reload

    used_nodes = set()

    # used_nodes will contain only the vertices that we want in the graph.
    # However, it contains all of the edges between those vertices, not
    # only those used in paths.

    log.info("Populating sources for shortest path... ")
    nodetypes = nx.get_node_attributes(graph, 'nodetype')
    p = r.pipeline()
    p.delete(SP_KEY)
    p.delete(USED_KEY)
    for node, nodetype in nodetypes.iteritems():

        if nodetype in ('relay', 'client', 'dest'):
            p.sadd(SP_KEY, node)
            p.hset(TYPE_KEY, node, nodetype)
    p.execute()

    log.info(Color.wrap("[complete]", Color.OKGREEN))

    log.info("Spawning 2 workers to process shortest paths... ")

    timer = ProgressTimer(int(r.scard(SP_KEY)))
    left = timer.total
    timer.tick(1)
    #pool = Pool(processes=12, initializer=thread_init,
                #initargs=(graphpath, SP_KEY, TYPE_KEY, USED_KEY, PATH_KEY, ))
    workers = []
    for x in xrange(2):
        p = multiprocessing.Process(
            target=concurrent.thread_shortest_path,
            args=(graphpath, SP_KEY, TYPE_KEY, USED_KEY, PATH_KEY, USED_KEY, ))

        p.start()
        workers.append(p)

    while left > 0:
        time.sleep(30)
        timer.tick((timer.total - left) - timer.total_done)
        sys.stderr.write(
            "{0} Spawning workers to process shortest paths... {1}"
            .format(Color.NEWL,
                    Color.wrapformat("[ETA: {0}]", Color.OKBLUE, timer.eta())))

        left = r.scard(SP_KEY)

    log.info("\nWaiting for jobs to terminate... ")
    for job in workers:
        p.join()
    #pool.join()
    log.info("Done")

    used_nodes = r.smembers(USED_KEY)
    if len(used_nodes) == 0:
      raise RuntimeError('Saved zero nodes after processing')
    core_graph = graph.subgraph(used_nodes)
    to_remove = list()

    def rev_tuple(tup):
        return (tup[1], tup[0])

    for edge in core_graph.edges_iter:
        if (not r.sismember(PATH_KEY, edge) and
                not r.sismember(PATH_KEY, rev_tuple(edge))):
            to_remove.append(edge)

    log.info("Kept %s vertices in the graph" % len(used_nodes))
    log.info("Removed %s extraneous edges" % len(to_remove))
    core_graph.remove_edges_from(to_remove)

    log.info("Re-trimming graph")
    collapse_graph_in_place(graph)
    log.info("Done")

    log.info("Writing core graph... ")
    nx.write_graphml(core_graph, "%s.xml" % args.save, prettyprint=True)

    try:
      nx.write_dot(core_graph, "%s.dot" % args.save)
    except:
      log.info("Failed to write dot graph")

    log.info(Color.wrap("[complete]", Color.OKGREEN))

    log.info("Cleaning up...")
    pipelined_delete(r, TYPE_KEY, USED_KEY, PATH_KEY)


def load_from_redis(r, args):
    """
    Create a igraph graph from redis
    """

    log.info("Loading from Redis")
    linkdict = LinkDict(r)
    vertices = VertexList()
    tor_vertices = set()
    graphlinks = []
    graphattrs = dict()
    graphattrs['latency'] = []
    stats = Stats({'non-pop-trim': int,
                   'unattachable-relays-count': int,
                   'relay-latency-defaulted': int,
                   'unattachable-relays': set,
                   'num-relays': int,
                   'num-pops': int,
                   'num-links': int,
                   'num-clients': int,
                   'client-connect-points': int})

    pipe = r.pipeline()
    i = 0
#Obtain the set of Tor relay IPs
    log.info("Reading Tor relays from %s... " % args.tor_relays)
    try:
        with open(args.tor_relays) as f:
          relays = json.load(f)

    except IOError as e:
        log.info("Error: [%s]" % e)
        raise

    log.info(Color.wrap("Done", Color.OKBLUE))

    log.info("Attaching clients to graph.")

    #Add clients
    if args.num_clients:

      clients_attached, client_attach_points = add_asn_endpoints(
          vertices,
          graphlinks,
          args.client_data,
          args.num_clients,
          'client')

      log.info("Attached {0} clients to {1} attachment points".format(
          clients_attached, client_attach_points))

    log.info("Attaching destinations to graph.")
    #Add dests
    if args.num_dests:
        dests_attached, dest_attach_points = add_alexa_destinations(
            vertices,
            graphlinks,
            args.num_dests)

    log.info("Attached {0} dests to {0} attachment points".format(
        dests_attached, dest_attach_points))

    protected = set()
    protected.update([relay['pop'] for relay in relays])
    protected.update(vertices.keys())

    # We want to trim all of the hanging edges of the graph.
    log.info("Trimming degree-1 vertices...")
    found_hanging_edge = True
    pass_ctr = 0

    while found_hanging_edge:
        pass_ctr += 1
        found_hanging_edge = False
        removed = set()
        n = 0
        timer = ProgressTimer(len(linkdict))
        for pop in linkdict.keys():

            if n % 100 == 0 or n == timer.total - 1:
                timer.tick(100)
                sys.stderr.write(
                    "{0}Pass {1}: {2} {3}".format(
                        Color.NEWL,
                        pass_ctr,
                        Color.wrapformat("[{0} processed, {1} trimmed]",
                                         Color.HEADER,
                                         n, stats['non-pop-trim']),
                        Color.wrapformat("[eta:{0}]",
                                         Color.OKGREEN,
                                         timer.eta())
                    ))

            n += 1

            if pop in removed:
                continue  # we saw this already
            if len(linkdict[pop]) >= 2:
                continue  # it can stay
            if pop in protected:
                continue  # We need relay/client/dest connect point

            # It's only connected to one
            connected = linkdict[pop].pop()
            removed.add(pop)
            del linkdict[pop]
            linkdict[connected].remove(pop)
            if len(linkdict[connected]) == 0:
                # This was a matched pair attached to nothing else
                del linkdict[connected]
                removed.add(connected)
            stats.incr('non-pop-trim')
            found_hanging_edge = True
        sys.stderr.write("\n")

    linkdict.collapse_degree_two(protected=protected)

    log.info("Trimmed {non-pop-trim} degree two hops".format(**stats))

#Set vertex id's for all of the pops we have links for.
    log.info("Adding PoPs...")
    for pop in linkdict.iterkeys():
        if pop in vertices:
            continue  # we have this one already.

        vertices.add_vertex(pop,
                            nodeid=pop,
                            nodetype='pop',
                            asn=r.get(dbkeys.POP.asn(pop)),
                            countries=r.smembers(dbkeys.POP.countries(pop)))

        stats.incr('num-pops')
        i += 1

    log.info(Color.wrapformat("Added [{0}]", Color.OKBLUE, stats['num-pops']))

    #Attach the relays
    for relay in relays:

        if relay['pop'] not in vertices:
          raise Exception("Matched relay to {0}, but couldn't find it "
                          "in vertices".format(relay['pop']))
          stats.incr('unattachable-relays-count')
          stats.incr('unattachable-relays', relay['relay_ip'])
          continue

        vertices.add_vertex(relay['relay_ip'],
                            nodeid=relay['relay_ip'],
                            nodetype='relay',
                            **relay)

        linkdelays = [
            delay
            for edge in r.smembers(dbkeys.Link.intralink(relay['pop']))
            for delay in r.smembers(dbkeys.delay_key(*eval(edge)))]

        try:
          deciles = util.decile_transform(linkdelays)
        except util.EmptyListError:
          deciles = [5 for x in xrange(10)]
          stats.incr('relay-latency-defaulted')

        graphlinks.append(EdgeLink(relay['relay_ip'], relay['pop'],
                          {'latency': deciles}))

        stats.incr('num-relays')
        tor_vertices.add(relay['relay_ip'])
        i += 1

    log.info("Added {0} relays. Did not attach {1} "
             "whose connection point was not linked to anything."
             .format(stats['num-relays'],
                     stats['unattachable-relays-count'])
             )
    log.info("Relays defaulted to 5ms links: [{0}]".format(
        stats['relay-latency-defaulted']))

    pipe.execute()

    already_processed = set()
    log.info("Processing links... ")
    i = 0

    for pop1 in linkdict.iterkeys():
        if pop1 not in vertices:
            continue

        for pop2 in linkdict[pop1]:
            if (pop2 not in vertices
                    or dbkeys.Link.interlink(pop1, pop2) in already_processed):
                continue

            linkkey = dbkeys.Link.interlink(pop1, pop2)
            linkdelays = [
                delay
                for edge in r.smembers(linkkey)
                for delay in r.smembers(dbkeys.delay_key(*eval(edge)))]

            try:
              latency = util.decile_transform(linkdelays)
            except util.EmptyListError:
              latency = float(r.get("graph:collapsed:%s" %
                                    (dbkeys.Link.interlink(pop1, pop2))))
            graphlinks.append(EdgeLink(pop1, pop2, {'latency': latency}))

            stats.incr('num-links')
            already_processed.add(dbkeys.Link.interlink(pop1, pop2))

        i += 1
        sys.stderr.write("{0}Processed links for {1} pops"
                         .format(Color.NEWL, i))

    log.info("Processed {0} pop links "
             .format(stats['num-links']))

    log.info("Making Graph")
    gr = nx.Graph()
    gr.add_nodes_from(vertices.nx_tuple_iter)
    gr.add_edges_from([edge.nx_tuple() for edge in graphlinks])

    try:
      bfs_edges = nx.bfs_edges(gr, linkdict.max_degree())
    except:
      print "Something was wrong with: %s" % linkdict.max_degree()
      raise

    bfs_node_gen = (node for pair in bfs_edges for node in pair)
    subgraph = gr.subgraph(bfs_node_gen)

    log.info("BFS reduced graph from %s to %s vertices".format(
             len(gr), len(subgraph)))

    log.info("Writing data file")
    nx.write_graphml(gr, args.reload)

    with open("vertices.dat", 'w') as vertout:
        vertices.write(vertout)

    log.info("Wrote files")

    log.info("STATS:")
    for key, val in stats.iteritems():
      log.info("{0}: {1}".format(key, val))

    return gr


def add_alexa_destinations(vertex_list, linklist, count):
    """
    Add potential destination endpoints based on the top 10000 destinations
    """
    r = connection.Redis()
    attached = 0
    pops = set()
    with pkg_resources.resource_stream(
            'inettopology_popmap.resources',
            'alexa_top_dests.txt') as destlist:

      for line in destlist:
        url, ip = line.split()

        db_ip_pop = dbkeys.get_pop(ip)

        if db_ip_pop is None:
          log.warn("Couldn't attach {0} with ip {1}. No matching IP found"
                   .format(url, ip))
          continue

        nodeid = "dest_{0}".format(ip.replace('.', '_'))
        if nodeid in vertex_list:
          continue  # Don't add the same url twice

        pops.add(db_ip_pop)
        vertex_list.add_vertex(nodeid,
                               nodeid=nodeid,
                               nodetype="dest",
                               url=url)

        linkkey = dbkeys.Link.intralink(db_ip_pop)

        linkdelays = [
            delay
            for edge in r.smembers(linkkey)
            for delay in r.smembers(dbkeys.delay_key(*eval(edge)))]

        try:
          latency = util.decile_transform(linkdelays)
        except util.EmptyListError:
          latency = [5 for x in xrange(10)]

        linklist.append(
            EdgeLink(nodeid,
                     db_ip_pop,
                     {'latency': latency}))

        attached += 1
        if attached >= count:
          break
    return (attached, len(pops))


def add_asn_endpoints(vertex_list, linklist, datafile, count, endpointtype):
    """
    Add endpoint nodes that connect to the graph based on ASNs.

    @param vertex_list: An instance of VertexList
    @type vertex_list: C{VertexList}
    @param linklist: A tuple containing a list of links, and an attribute
        dictionary
    @type linklist: C{tuple}
    @param datafile: Path to a datafile that contains at least
        two columns called 'Number' and 'ASN'. The 'Number'
        column should indicate the relative number of endpoints
        that should connect to that ASN.
    @type datafile: C{str}

    @param count: The number of endpoints to add.
    @type count: C{int}
    @param endpointtype: The label for the endpoint. 'client' is a
        good example.
    @type endpointtype: C{str}
    """
    r = connection.Redis()
    try:
        cdata_file = datautil.DataFile(datafile, sep="|")
        cdata_file.add_index('ASN')
    except IOError:
        logging.error(Color.fail("Error reading %s" % datafile))
        sys.exit(-1)

    attach = dict()
    for asn in cdata_file['ASN']:
        try:
            pop = find_pop_for_asn(asn)
            attach[asn] = (pop, cdata_file['ASN'][asn][0]['Number'])
        except graph_objects.ASNNotKnown:
            pass

    def node_id(asn, unique):
        return "%s_%s_%s" % (endpointtype, asn, unique)

    counter = 0
    if len(attach) == 0:
        sys.stderr.write(Color.fail(
            "[failed] No %s could be attached.\n" % endpointtype))
        logging.error(Color.fail(
            "[failed] No %s could be attached." % endpointtype))

    else:
        total = sum(map(lambda x: int(x[1]), attach.itervalues()))
        for asn, data in attach.iteritems():
            num_to_attach = round(count * (float(data[1]) / float(total)))
            for j in xrange(0, int(num_to_attach)):

                vertex_list.add_vertex(node_id(asn, j), nodeid=node_id(asn, j),
                                       nodetype=endpointtype, asn=asn)
                linkkey = dbkeys.Link.intralink(data[0])

                linkdelays = [
                    delay
                    for edge in r.smembers(linkkey)
                    for delay in r.smembers(dbkeys.delay_key(*eval(edge)))]

                try:
                  latency = util.decile_transform(linkdelays)
                except util.EmptyListError:
                  latency = [5 for x in xrange(10)]

                linklist.append(EdgeLink(node_id(asn, j),
                                data[0], {'latency': latency}))
                counter += 1

        log.info(Color.wrapformat("Success [{0} attached]", Color.OKBLUE,
                                  counter))
    return (counter, len(attach))


def collapse_graph_in_place(graph):
    """ Collapse any degree two nodes in the graph *in-place*

    Specifically, collapse degree two nodes where the ASN
    of the node matches the ASN of the node on the other side.
    """
    asns = nx.get_node_attributes(graph, 'asn')
    types = nx.get_node_attributes(graph, 'nodetype')
    collapseable = True

    ctr = 0
    while collapseable:
        ctr += 1
        logging.info("Pass %s" % ctr)
        to_collapse = []
        zero_length = 0
        exempt = set()
        for node in nx.nodes_iter(graph):
            if types[node] != 'pop':
                continue

            neighbors = graph.neighbors(node)
            if len(neighbors) == 2:
                # This link is collapsible if the asn matches that of
                # one of its neighbors **and** none of the parties
                # have already been collapsed on this pass
                if ((asns[node] == asns[neighbors[0]]
                   or asns[node] == asns[neighbors[1]])
                   and len(exempt & (set([node]) | set(neighbors))) == 0):
                    to_collapse.append(node)
                    exempt |= set(neighbors)
                    exempt |= set([node])

        if len(to_collapse) != 0:
            logging.info("Collapsing %s nodes" % len(to_collapse))

            for node in to_collapse:
                neighbors = graph.neighbors(node)
                s1_weight = float(graph[node][neighbors[0]]['latency'])
                s2_weight = float(graph[node][neighbors[1]]['latency'])
                graph.add_edge(*neighbors, latency=s1_weight + s2_weight)
                zero_length += 1 if s1_weight + s2_weight == 0 else 0
                graph.remove_node(node)
            logging.info("Created {0} new links. {1} with zero latency"
                         .format(len(to_collapse), zero_length))
        else:
            collapseable = False


def find_pop_for_asn(asn):
  r = connection.Redis()

  pops = r.smembers(dbkeys.ASN.pops(asn))

  if len(pops) == 0:
      raise graph_objects.ASNNotKnown("%s" % asn)

  if len(pops) == 1:
      return pops.pop()

  popsize = [(r.scard(dbkeys.POP.members(pop)), pop)
             for pop in pops]

  return sorted(popsize, key=operator.itemgetter(0))[-1][1]
