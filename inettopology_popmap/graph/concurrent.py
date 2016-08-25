import logging
log = logging.getLogger(__name__)

import sys
import os
import networkx as nx

import inettopology_popmap.connection as connection
import inettopology_popmap.data.dbkeys as dbkeys
from inettopology.util.general import Color, pairwise
import inettopology_popmap.graph.pqueue as pqueue


class ValleyFreeError(Exception):
  pass


def redirect_output():
  """ Open a file and redirect all output to it """
  log_out = open('pool_%s.log' % os.getpid(), 'w')
  sys.stderr = log_out
  sys.stdout = log_out
  return log_out


def thread_shortest_path(graphpath, sp_key, type_key,
                         node_key, path_key, used_key):

    global thread_graph

    log_out = redirect_output()

    log_out.write("Spawned with sp_key = {0}; type_key = {1}; "
                  "node_key={2}; path_key={3}\n"
                  .format(sp_key, type_key, used_key, path_key))
    log_out.flush()

    r = connection.Redis()

    log_out.write("Reading graph...")
    log_out.flush()

    thread_graph = nx.read_graphml(graphpath)

    log_out.write(" Complete\n")
    log_out.flush()

    target = r.spop(sp_key)
    while target:
        used_nodes = set()
        used_paths = set()
        log_out.write("Obtaining shortest paths for %s... " % target)
        log_out.flush()

        paths = nx.single_source_dijkstra_path(
            thread_graph, target, weight='med_latency')

        for path_target, path in paths.iteritems():
          if r.hget(type_key, path_target) in ('relay', 'client', 'dest'):
            # We only care for relays, clients and destinations
            try:
              errors, total = check_valley_free(
                  thread_graph, path, log=log_out)

              if total > 0:
                log_out.write("Path from %s to %s was valley-free. "
                              " %0.0f/%0.0f (%0.2f) missing links in calc\n"
                              % (target, path_target,
                                 errors, total, errors / total))

            except ValleyFreeError:
              # If it's not valley free, we rebuild a new path
              log_out.write("Path from %s to %s is not valley-free. "
                            "Building a new one... "
                            % (target, path_target))

              try:
                path, time_taken = valley_free_path(
                    thread_graph, target, path_target)

                log_out.write(" finished in %s seconds. New Path: %s "
                              "[%0.0f/%0.0f (%0.2f%%) of links had no "
                              "information]\n"
                              % (Color.wrap(time_taken, Color.OKBLUE),
                                 str(path), path.errct, len(path) - 1,
                                 float(path.errct) / float(len(path) - 1)))

              except ValleyFreeError:
                log_out.write(
                    Color.warn(
                        "Couldn't produce valley-free path from %s to %s\n"
                        % (target, path_target)))
                raise

            else:
              # Now store the links and nodes from this
              # path so that we keep them.
              used_nodes.update(path)
              hops = list()
              for hop in pairwise(path):
                  hops.append(hop)
                  #Add each hop to our set of paths
                  used_paths.add(hop)

        r.sadd(path_key, *used_paths)
        r.sadd(used_key, *used_nodes)
        log_out.write("Done\n")
        log_out.flush()
        target = r.spop(sp_key)

    log_out.write("Exiting ")
    log_out.flush()
    log_out.close()
    sys.exit(0)


def check_valley_free(g, path, log=None):
  r = connection.Redis()

  if r.get(dbkeys.AS.status('peering_data')) != "True":
    sys.stderr.write("Attempted to check valley-free property, "
                     "but have no peering data.\n")
    return (0, 1)  # IF we have no peering data. It's irrelevant.

  if len(path) == 0:
    return (0, 1)

  went_down = False
  errct = 0.0
  hopct = 0.0
  asn_path = [g.node[hop]['asn']
              for hop in path
              if g.node[hop]['asn'] != "N/A"]

  for as1, as2 in pairwise(asn_path):

    hopct += 1
    if as1 == as2:
      continue

    relationship = r().hget(dbkeys.AS.relationship(as1), as2)

    if not relationship:
      # Let's try the other side.
      relationship = r().hget(dbkeys.AS.relationship(as2), as1)
      if not relationship:
        if log:
          log.write("No relationship for %s <-> %s\n" % (as1, as2))
        errct += 1
        continue
      else:
        # We need to swap -1 and 1 for this side
        # since it's the opposite perspective.
        # If we swap a 2 to a -2, who cares.
        relationship *= -1

    if relationship == 1:
      went_down = True

    elif relationship == -1:  # AS1 is a customer of AS2
      if went_down:
        raise ValleyFreeError()

  return (errct, hopct)


class vfp(list):
  def __init__(self):
    list.__init__(self)
    self.entered_valley = False
    self.errct = 0


def valley_free_path(g, start, path_target):
  """
  This is effectively a modified breadth first search implementation
  which searches available paths, and truncates them when they violate
  the valley-free property.
  """
  r = connection.Redis()

  q = pqueue.pqueue()
  q.push(vfp([start]), 0)

  paths_found = pqueue.pqueue()

  while not q.empty():
    try:
      # Get the lowest distance path.
      dist, path = q.pop()
    except IndexError:
      break
    else:
      for linked_node in g[path[-1]]:
        if linked_node not in path:  # Don't go backwards
          newpath = path
          newpath.append(linked_node)
          as1 = g.node[path[-1]]['asn']
          as2 = g.node[linked_node]['asn']

          """ If this path enters a valley, fine. If it
          enters a valley and tries to come back up, just remove it"""
          relationship = r.hget(dbkeys.AS.relationship(as1), as2)
          if not relationship:
            # Let's try the other side.
            relationship = r.hget(dbkeys.AS.relationship(as2), as1)
            if not relationship:
              newpath.errct += 1
              continue
            else:
              # We need to swap -1 and 1 for this
              # side since it's the opposite perspective.
              # If we swap a 2 to a -2, who cares.
              relationship *= -1

          if relationship == -1:  # AS1 is a customer of AS2
            if newpath.entered_valley:  # This path is no good
              continue

          elif relationship == 1:
            newpath.entered_valley = True

          if linked_node == path_target:
            paths_found.push(newpath,
                             dist + g[path[-1]][linked_node]['latency'])

            if all(map(lambda x: x[0] > paths_found.peek(), q.queue)):
              return paths_found.pop()

          else:
            q.push(newpath, dist + g[path[-1]][linked_node]['latency'])

  try:
    return paths_found.pop()
  except IndexError:
    raise ValleyFreeError()
