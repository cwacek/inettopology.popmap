__all__ = ('delay_key', 'ASN', 'POP', 'Link', 'AS')

import logging
log = logging.getLogger(__name__)
import inettopology.util.decorators
import inettopology.util.structures as structures
import inettopology_popmap.connection as connection
import inettopology_popmap.data.preprocess as preprocess
from inettopology_popmap.data import DataError


@inettopology.util.decorators.factory
def mutex_popnum():
  return structures.RedisMutex(connection.Redis(), 'mkpop')


@inettopology.util.decorators.factory
def mutex_popjoin():
  return structures.RedisMutex(connection.Redis(), 'popjoin')


def delay_key(ip1, ip2):
    """
    Return a delay key, with the 'lower' ip always first
    """
    return "ip:links:%s:%s" % ((ip1, ip2) if ip1 < ip2 else (ip2, ip1))


def ip_key(ip):
  return "ip:%s" % ip


def get_pop(ip, pipe=None):
  p = pipe if pipe else connection.Redis()
  return p.hget(ip_key(ip), 'pop')


def get_delay(link):
  delays = list(connection.Redis().smembers(link))
  return float(sorted(delays)[len(delays) / 2])


def setpopnumber(mutex, key, pipe=None):
  """ Atomically set the popnumber for a :key: by
  using :mutex: to lock
  This ensures we don't have overlap among popnumbers.
  """
  r = connection.Redis()

  p = mutex.backend().pipeline() if not pipe else pipe
  pop = r.incr(POP.counter())

  p.sadd(POP.list(), pop)
  p.sadd(POP.members(pop), key)
  p.hset(ip_key(key), 'pop', pop)

  asn = r.hget(ip_key(key), 'asn')
  if not asn:
    raise DataError("IP '%s' is missing an ASN" % key)

  try:
    aslookup = preprocess.MaxMindGeoIPReader.Instance()
    cc = aslookup.lookup_country_codes(key)
    p.sadd(POP.countries(pop), *cc)
    log.debug("Setting countrycode for {0} to {1}".format(pop, cc))
  except Exception as e:
    log.critical("Failed to lookup country codes: {0}".format(e))

  p.set(POP.asn(pop), asn)
  p.sadd(ASN.pops(asn), pop)

  if not pipe:
      p.execute()
  return pop


class ASN:

  @staticmethod
  def pops(asn):
    return "asn:%s:pops" % asn


class POP:
  @staticmethod
  def joined(pop):
    return "pop:%s:joined" % pop

  @staticmethod
  def asn(pop):
    return "pop:%s:asn" % pop

  @staticmethod
  def countries(pop):
    return "pop:%s:cc" % pop

  @staticmethod
  def neighbors(pop):
    return "pop:%s:connected" % pop

  @staticmethod
  def members(pop):
    return 'pop:%s:members' % pop

  @staticmethod
  def counter():
    return 'popincr'

  @staticmethod
  def list():
    return 'poplist'


class Link:

  @staticmethod
  def interlink(a, b):
    if int(a) < int(b):
        return "links:inter:%s:%s" % (a, b)
    return "links:inter:%s:%s" % (b, a)

  @staticmethod
  def intralink(a):
    return "links:intra:%s" % a

  @staticmethod
  def unassigned():
    return "delayed_job:unassigned_links"

  @staticmethod
  def unassigned_fails():
    return "delayed_job:unassigned_link_fails"

  @staticmethod
  def processed():
    return "delayed_job:unassigned_links"

  @staticmethod
  def ensure_dbsafe(link):
      if len(link) != 2:
          return link[0:2]
      return link


class AS:
  metakeys = {'peering_data': 'peering_data_loaded'}

  @staticmethod
  def relationship(asn):
    return "as:%s:peering" % str(asn)

  @staticmethod
  def status(subkey):
    return "as:meta:%s" % AS.metakeys[subkey]
