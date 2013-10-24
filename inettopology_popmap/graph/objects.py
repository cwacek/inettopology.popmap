import logging
log = logging.getLogger(__name__)

import sys
import itertools

import inettopology_popmap.connection as connection
from inettopology_popmap.data.cleanup import write_failed
from inettopology.util.general import ProgressTimer, Color
import inettopology_popmap.data.dbkeys as dbkeys


def decile_transform(input_list):
  sorted_list = sorted(input_list)

  deciles = list()
  interval = len(sorted_list) / 10

  if len(sorted_list) == 0:
    raise EmptyListError()
  else:
    for decile in xrange(10):
      deciles[decile] = sorted_list[decile * interval]


class EmptyListError(Exception):
    pass


class ASNNotKnown(Exception):
    pass


class DuplicateVertex(Exception):
    pass


class LinkDict(dict):
  def __init__(self, r):
    logging.info("Initializing Link Dictionary...")
    self._links = r.keys("links:inter:*")
    self._max_degree = (-1, 0)

    for link in self._links:
      link_eps = link.split(":")[2:]
      try:
        self[link_eps[0]].add(link_eps[1])
        if len(self[link_eps[0]]) > self._max_degree[1]:
          self._max_degree = (link_eps[0], len(self[link_eps[0]]))
      except:
        self[link_eps[0]] = set([link_eps[1]])
      try:
        self[link_eps[1]].add(link_eps[0])
        if len(self[link_eps[1]]) > self._max_degree[1]:
          self._max_degree = (link_eps[1], len(self[link_eps[1]]))
      except:
        self[link_eps[1]] = set([link_eps[0]])

      if len(self[link_eps[0]]) == 0 or len(self[link_eps[1]]) == 0:
        raise Exception("Link degree should not be zero")

  def max_degree(self):
    return self._max_degree[0]

  def max_degree_num(self):
    return self._max_degree[0]

  def collapse_degree_two(self, protected=[]):
    log.info("Cleaning up collapse dbkeys...")
    r = connection.Redis()
    p = r.pipeline()
    for key in r.keys("graph:collapsed:*"):
      p.delete(key)
    write_failed(p.execute())

    pass_ctr = 0
    collapsable = True
    ignoreable = set()
    clogout = open('collapse.log', 'w')
    while collapsable:
      pass_ctr += 1
      sys.stderr.write("\n")
      collapsable = False
      degree2nodes = filter(
          lambda val: (len(val[1]) == 2 and val[0] not in ignoreable),
          self.iteritems())

      counter = 0
      n = 0
      deferred = 0
      collapsed = set()
      timer = ProgressTimer(len(degree2nodes))

      for node, connections in degree2nodes:

        if n % 50 == 0 or n == timer.total - 1:
          timer.tick(50)
          sys.stderr.write(
              "{0}Pass {1}: {2} {3}".format(
                  Color.NEWL, pass_ctr,
                  Color.wrapformat(
                      "[{0} processed, {1} collapsed, {2} deferred]",
                      Color.HEADER, n, counter, deferred
                  ),
                  Color.wrapformat(
                      "[eta: {0}]",
                      Color.OKGREEN, timer.eta()
                  ))
          )

        n += 1

        asns = [r.get(dbkeys.POP.asn(x)) for x in connections | set([node])]

        same_asn = reduce(lambda x, y: x if x == y else False, asns)
        if (same_asn is False or node in protected):
          ignoreable.update(connections | set([node]))
          continue

        if len(collapsed & (connections | set([node]))) != 0:
          deferred += 1
          continue

        collapsed.update(connections | set([node]))
        side1 = connections.pop()
        side2 = connections.pop()
        connections.update(set([side1, side2]))

        try:
          #side1_delay = median(get_delays(dbkeys.Link.interlink(node, side1)))
          side1_delays = sorted(
              [delay
               for edge in r.smembers(dbkeys.Link.interlink(node, side1))
               for delay in r.smembers(dbkeys.delay_key(*eval(edge)))])
        except:
          side1_delay = r.get("graph:collapsed:%s" %
                              (dbkeys.Link.interlink(node, side1)))
        try:
          #side2_delay = median(get_delays(dbkeys.Link.interlink(node, side2)))
          side2_delays = sorted(
              [delay
               for edge in r.smembers(dbkeys.Link.interlink(node, side2))
               for delay in r.smembers(dbkeys.delay_key(*eval(edge)))])
        except:
          side2_delay = r.get("graph:collapsed:%s" %
                              (dbkeys.Link.interlink(node, side2)))

        combined_delays = decile_transform(
            [comb for comb
             in itertools.imap(sum, side1_delays, side2_delays)])

        r.set('graph:collapsed:%s' % (dbkeys.Link.interlink(*list(connections))),
              combined_delays)

        clogout.write("Collapsed %s [%s ms] %s [%s ms] %s\n" %
                      (side1, side1_delay, node, side2_delay, side2))

        collapsable = True

        del self[node]
        self[side1].add(side2)
        self[side2].add(side1)
        self[side1].remove(node)
        self[side2].remove(node)

        counter += 1

    clogout.close()


class VertexList(dict):
    def __init__(self):
      self._available_attrs = set()

    def nx_tuple_iter(self):
      for node in self:
        yield (node, self[node])

    def attrs_for(self, vid):
      """ Return the attribute dictionary for vertex 'vid'. """
      return self[vid]

    def write(self, f):
        for vertex, attrdict in self.iteritems():
            f.write("%s " % (vertex))
            for attr, val in attrdict.iteritems():
                f.write("%s=%s " % (attr, val))
            f.write("\n")

    def add_vertex(self, vid, **kwargs):
        if vid in self:
            raise DuplicateVertex("%s" % vid)
        self[vid] = kwargs
        self._available_attrs |= set(kwargs.keys())

    def get_by_attr(self, attr, *types):
        """
        Return the a list of IDs of vertexes where the
        value of <b>attr</b> is in <b>types</b>.
        """
        if attr not in self._available_attrs:
          return []
        retlist = []
        for vid, attrs in self.iteritems():
          try:
            if vid[attrs] in types:
              retlist.append(vid)
          except KeyError:
            #It's okay to not find one, we just won't return it.
            pass
        return retlist


class EdgeLink(object):
  def __init__(self, end1, end2, attrs=dict()):
    self.pair = (end1, end2)
    self.attrs = attrs

  def add_attribute(self, name, value):
    self.attrs[name] = value

  def nx_tuple(self):
    return (self.pair[0], self.pair[1], self.attrs)


class Stats(dict):
    def __init__(self, keys):
        self.__keytypes = dict()
        for key, keytype in keys.iteritems():
            if isinstance(keytype, basestring):
                self[key] = ''
                self.__keytypes[key] = basestring
            elif keytype == set:
                self[key] = set()
                self.__keytypes[key] = set
            elif keytype == dict:
                self[key] = dict()
                self.__keytypes[key] = dict
            else:
                self[key] = 0
                self.__keytypes[key] = int

    def incr(self, key, val=None):
        if val:
            try:
                self[key].add(val)
            except TypeError:
                self[key] += val
        else:
            self[key] += 1
