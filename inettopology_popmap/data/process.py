import logging
log = logging.getLogger(__name__)

import sys
import os
import itertools
import redis
redis_errors = (redis.ConnectionError,
                redis.InvalidResponse,
                redis.ResponseError)

from inettopology import SilentExit
from inettopology.util.general import ProgressTimer, Color
import inettopology.util.structures as structures
import inettopology.util.decorators
import inettopology_popmap.data.dbkeys as dbkeys
from inettopology_popmap.data.parsers import TraceParser
import inettopology_popmap.connection as connection


class DataError(Exception):
  pass


def print_unless_seen(text, seenset):
  if text not in seenset:
    print text
    seenset.add(text)


def load_link_pairs(newpairs):
  r = connection.Redis()
  for link in newpairs:
    if link[0] == link[1]:
      raise Exception("Should not happen")
    if not r().exists(dbkeys.delay_key(link[0], link[1])):
      r().lpush("delayed_job:unassigned_links",
                dbkeys.delay_key(link[0], link[1]))
    r().sadd(dbkeys.delay_key(link[0], link[1]), link[2])


def parse(args):

  redis_conn = connection.Redis()
  redis_conn.config(structures.ConnectionInfo(**args.redis))

  try:
    with open(args.trace) as trace_in:
      tracehops = []
      seenset = set()
      numtraces = 0
      log.info("Processed trace: %s" % str(numtraces))
      for line in trace_in:
        if line.split()[0] == "traceroute":
          numtraces += 1
          if numtraces % 1000 == 0:
            log.info("\r\x1b[K" + "Processed trace: %s" % str(numtraces))
          try:
            newpairs, removed = TraceParser.parse(tracehops)
            if removed is not None:
              log.debug("Removed %s" % removed)
          except TraceParser.EmptyTraceError:
            tracehops = [line]
            continue
          if not args.dump and dbkeys.mutex_popjoin().is_locked():
            log.debug("Waiting for popjoin lock")
            dbkeys.mutex_popjoin().wait()

          if args.dump:
            for pair in newpairs:
              print_unless_seen(pair[0], seenset)
              print_unless_seen(pair[1], seenset)
          else:
            load_link_pairs(newpairs)

          tracehops = [line]
        else:
          tracehops.append(line)

  except IOError as e:
    log.error("Error: {0}".format(e))
    raise SilentExit()
  except DataError as e:
    log.error("Error: {0}".format(e))
    raise SilentExit()


def descend_target_chain(r, target):
  """ If :target: has already been joined to something,
  descend down the list of POPs it's been joined to
  until we find the bottom one.
  """

  target_recurse = r.get(dbkeys.POP.joined(target))

  if target_recurse is None:
    return target

  visited = set()
  bottom = target

  while target_recurse is not None:
    bottom = target_recurse
    visited.add(bottom)
    target_recurse = r.get(dbkeys.POP.joined(bottom))

  if not r.sismember(dbkeys.POP.list(), bottom):
    raise IndexError("Bottom of target chain wasn't valid. "
                     "{0} is not a member of the poplist".format(bottom))

  p = r.pipeline()
  # Set all of the 'joined' keys for the
  # nodes we visited to the bottom one
  for node in visited:
    if node is not None and node is not bottom:
      p.set(dbkeys.POP.joined(node), bottom)

  p.execute()
  p.reset()
  return bottom


def process_delayed_joins(args):
  r = connection.Redis()
  if r.llen("delayed_job:unassigned_link_fails") > 0:
    sys.stderr.write("Have unassigned links. "
                     "Run assign_pops --process_failed\n")
    raise SilentExit()
  # Now we process any joins that need to happen. First we lock.
  error_ctr = 0
  dbkeys.mutex_popjoin().acquire()
  joinlist = preprocess_joins()
  inprocess = list()
  x = len(joinlist)
  log.info("Joining pop pairs: %d".format(x))

  fh = None
  if args.log_joins:
    fh = logging.FileHandler(args.log_joins, mode='w')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter('%(message)s'))
    log.addHandler(fh)

  timer = ProgressTimer(x)
  for i, to_join in enumerate(joinlist):
    inprocess.append(to_join)
    log.info("Joining %s to %s\n" % (to_join[1], to_join[0]))

    try:
      joined = join_pops(r(), to_join[0], to_join[1])
    except redis_errors as e:
      log.error("Encountered error while processing: {0}. [{1}]\n"
                .format(to_join, e))
      joinlist.insert(0, inprocess.pop())
      error_ctr += 1
      continue

    else:
      if joined is not None:
        log.info("Joined %s to %s\n" % (joined[1], joined[0]))

      if (r.sismember(dbkeys.POP.list(), to_join[1])
         or r.exists(dbkeys.POP.members(to_join[1]))):

        if descend_target_chain(r(), to_join[0]) != to_join[1]:
          raise Exception("Join Failed in ways it should not have...")
        else:
          log.info("Did not join {0} to {1} because {2} had "
                   "previously been joined to {3}\n"
                   .format(to_join[1], to_join[0], to_join[0], to_join[1]))
      timer.tick(1)

    x = len(joinlist) - i

    sys.stderr.write("{newl} {0} joins left {1}".format(
                     x,
                     Color.wrapformat("[{0} seconds to finish]",
                                      Color.OKBLUE, timer.eta()),
                     newl=util.Color.NEWL))

  r.delete('delayed_job:popjoins')
  r.delete('delayed_job:popjoins:inprocess')
  r.delete(r.keys(dbkeys.POP.joined("*")))
  dbkeys.mutex_popjoin().release()
  log.info("Joined pops with %d errors while processing" % error_ctr)

  if fh is not None:
    log.removeHandler(fh)


def preprocess_joins():
  r = connection.Redis()
  numjoins = r.llen('delayed_job:popjoins')
  joins = map(eval, r.lrange('delayed_job:popjoins', 0, -1))
  jm = dict()
  reduced_joins = list()
  seen_joins = set()
  log.info("Preprocessing %s joins" % numjoins)

  def get_join_target(jm, node):
    target = node
    seen = set()
    try:
      while True:
        seen.add(target)
        target = jm[target]
    except KeyError:
      pass
    seen.remove(target)
    if len(seen) > 1:
      jm.update(itertools.izip_longest(seen, [target], fillvalue=target))
    return target

  timer = ProgressTimer(numjoins)
  for i, join in enumerate(joins):
    from_node = get_join_target(jm, join[1])
    to_node = get_join_target(jm, join[0])
    if from_node != to_node:
      jm[from_node] = to_node

    if i % 100 == 0:
      timer.tick(100)
      sys.stderr.write(Color.NEWL + "PreProcessing joins {0}"
                       .format(Color.wrapformat("[ETA: {1} seconds]",
                                                Color.OKBLUE, timer.eta())))

  sys.stderr.write(Color.NEWL +
                   "PreProcessing joins {0}\n"
                   .format(Color.wrapformat("[complete]", Color.OKGREEN)))

  timer = ProgressTimer(numjoins)
  for i, join in enumerate(joins):
    newjoin = (get_join_target(jm, join[0]), join[1])

    if newjoin not in seen_joins:
      reduced_joins.append(newjoin)
      seen_joins.add(newjoin)

    if i % 100 == 0:
      timer.tick(100)
      sys.stderr.write(Color.NEWL + "Reducing join list {0}".format(
                       Color.wrapformat("[ETA: {0} seconds]",
                                        Color.OKBLUE, timer.eta())))

  sys.stderr.write(Color.NEWL +
                   "Reducing join list {0}\n"
                   .format(Color.wrapformat("[complete]", Color.OKGREEN)))

  log.info("Reduced join list from %s to %s joins"
           % (numjoins, len(reduced_joins)))

  return reduced_joins


def join_pops(r, newpop, oldpop):
  """
  Join oldpop to newpop.
  """

  if newpop == oldpop:
    return

  if not r.sismember(dbkeys.POP.list(), newpop):
    raise Exception("%s is not in the poplist\n", newpop)

  members = r.smembers(dbkeys.POP.members(oldpop))
  popas = r.get(dbkeys.POP.asn(oldpop))
  interlinks = r.smembers(dbkeys.POP.neighbors(oldpop))

  pipe = r.pipeline()
  for connected_pop in interlinks:
    if connected_pop == newpop:
      # What used to be an inter link from oldpop -> newpop
      # needs to become an intra link
      intralinkdata = r.smembers(dbkeys.Link.interlink(connected_pop,
                                 oldpop))
      store_link(r, map(eval, intralinkdata),
                 newpop, pipe=pipe, multi=True)

    else:
      # The inter link between connected -> oldpop needs to be
      # redirected to point at newpop
      interlinkdata = r.smembers(dbkeys.Link.interlink(connected_pop, oldpop))

      if len(interlinkdata) == 0:
        raise IndexError("Link between {0} and {1} has no links".format(
                         connected_pop, oldpop))

      store_link(r, map(eval, interlinkdata),
                 newpop, connected_pop, pipe=pipe, multi=True)

    pipe.delete(dbkeys.Link.interlink(connected_pop, oldpop))
    pipe.srem(dbkeys.POP.neighbors(connected_pop), oldpop)

  #Move every intra link that used to be in oldpop to newpop
  store_link(r, map(eval, r.smembers(dbkeys.Link.intralink(oldpop))),
             newpop, pipe=pipe, multi=True)

  # Update the pop value for every member of oldpop, and move it to newpop
  for member in members:
    pipe.hset(dbkeys.ip_key(member), 'pop', newpop)
    pipe.smove(dbkeys.POP.members(oldpop), dbkeys.POP.members(newpop), member)

  # Clean up oldpop
  pipe.delete(dbkeys.POP.members(oldpop))
  pipe.delete(dbkeys.POP.neighbors(oldpop))
  pipe.delete(dbkeys.Link.intralink(oldpop))
  pipe.srem(dbkeys.ASN.pops(popas), oldpop)
  pipe.delete(dbkeys.POP.asn(oldpop))
  pipe.srem(dbkeys.POP.list(), oldpop)

  # Mark it as joined
  pipe.set(dbkeys.POP.joined(oldpop), newpop)
  pipe.rpush("join:history", "%s => %s" % (oldpop, newpop))
  pipe.execute()
  return (newpop, oldpop)


def store_link(r, link, pop1, pop2=None, pipe=None, multi=False):
  p = connection.Redis().pipeline() if pipe is None else pipe

  if len(link) == 0:
    return

  if pop2 and pop1 != pop2:
    p.sadd(dbkeys.POP.neighbors(pop1), pop2)
    p.sadd(dbkeys.POP.neighbors(pop2), pop1)
    p.sadd(dbkeys.Link.interlink(pop1, pop2),
           *link if multi else [dbkeys.Link.ensure_dbsafe(link)])
  else:
    p.sadd(dbkeys.Link.intralink(pop1),
           *link if multi else [dbkeys.Link.ensure_dbsafe(link)])

  if pipe is None:
    p.execute()
