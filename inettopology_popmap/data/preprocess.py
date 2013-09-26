import re
import pkg_resources
import logging
log = logging.getLogger(__name__)

from inettopology import SilentExit
import inettopology_popmap.connection as connection
import inettopology_popmap.data.dbkeys as dbkeys
import inettopology.util.decorators


@inettopology.util.decorators.singleton
class MaxMindGeoIPReader(object):

  def __init__(self):
    try:
      import pygeoip
      import pdb; pdb.set_trace()
      fname = pkg_resources.resource_filename('inettopology_popmap',
                                              'GeoIPASNum.dat')
      self._db = pygeoip.GeoIP(fname, pygeoip.MEMORY_CACHE)
    except IOError, e:
      raise Exception("Failed to open GeoIP database [{0}]".format(e))
    except ImportError:
      raise Exception("IP Translation requires the pygeoip library: "
                      "'pip install pygeoip'")

  def lookup_ips(self, ips):
    return map(self._db.org_by_addr, ips)

  def lookup_country_codes(self, ips):
    return map(self._db.country_code_by_addr, ips)


def load_attr_data(args):
  r = connection.Redis()
  try:
    keys = None
    i = 0
    with open(args.attr_file) as f:
      for i, line in enumerate(f):
        fields = line.split()
        if len(fields) == 0:
          continue
        if i == 0:
          if fields[0] == "#":
            keys = fields[1:]
            continue
        ip = fields[0]
        if keys:
          vals = dict([pair for pair in zip(keys, fields[1:])
                       if pair[0] != 'pop'])
        else:
          vals = dict(zip(fields[1::2], fields[2::2]))
          if 'pop' in vals:
            del vals['pop']

        r.hmset(dbkeys.ip_key(ip), vals)
        r.sadd('iplist', ip)
        if i % 10000 == 0:
          log.info("Set values for %d" % i)

  except IOError as e:
    log.error("Error: %s" % e)
    raise SilentExit
  except Exception as e:
    log.warn("\nError parsing line [%s]: %s\n" % (re.escape(line), e))
