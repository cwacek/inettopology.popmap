import logging
log = logging.getLogger(__name__)

import inettopology_popmap.connection as connection
from inettopology_popmap.data import dbkeys
import inettopology.util.structures as structures
from inettopology.util.general import Color


def pipelined_delete(r, *keys):
  p = r.pipeline()
  for key in keys:
    p.delete(key)
  result = p.execute()
  for key, res in zip(keys, result):
    log.info("Deleting '{0}'... {0}".format(key,
             Color.wrap('[failure]', Color.FAIL) if not res else
             Color.wrap('[success]', Color.OKBLUE)))


def cleanup(args):
  """
  Clean all of the pop and link related information out of the database.
  """
  r = connection.Redis(structures.ConnectionInfo(**args.redis))

  def write_failed(result):
    if not all(result):
      log.info(Color.wrapformat("[failed]", Color.FAIL))
    else:
      log.info(Color.wrapformat("[{0} removed]", Color.OKBLUE, len(result)))

  log.info("Removing IP pop data (may take a while)... ")
  ips = r.smembers('iplist')
  p = r.pipeline()
  for ip in ips:
    p.hdel(dbkeys.ip_key(ip), 'pop')
  result = p.execute()
  del ips
  log.info(Color.wrapformat("[{0} removed]",
                            Color.OKBLUE,
                            len(filter(None, result))))

  log.info("Removing PoP link data (may take a while)... ")
  linkkeys = r.keys("links:*")
  p = r.pipeline()
  for key in linkkeys:
    p.delete(key)
  result = p.execute()
  write_failed(result)
  del linkkeys

  log.info("Removing pop keys... ")
  popkeys = r.keys("pop:*")
  p = r.pipeline()
  for key in popkeys:
    p.delete(key)
  result = p.execute()
  write_failed(result)
  del popkeys

  log.info("Removing asn keys... ")
  popkeys = r.keys("asn:*")
  p = r.pipeline()
  for key in popkeys:
    p.delete(key)
  result = p.execute()
  write_failed(result)
  del popkeys

  if args.ip_links:
    log.info("Removing ip links... ")
    p = r.pipeline()
    p.delete(dbkeys.Link.unassigned())
    p.delete(dbkeys.Link.unassigned_fails())
    p.delete("delayed_job:processed_links")
    iplinkkeys = r.keys("ip:links:*")
    for key in iplinkkeys:
      p.delete(key)
    result = p.execute()
    write_failed(result)

  pipelined_delete(r,
                   'poplist',
                   'join:history',
                   'delayed_job:popjoins',
                   'delayed_job:popjoins:known',
                   'popincr',
                   'mutex:popjoin:init')
