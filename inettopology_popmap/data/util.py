

def delay_key(ip1, ip2):
    """
    Return a delay key, with the 'lower' ip always first
    """
    return "ip:links:%s:%s" % ((ip1, ip2) if ip1 < ip2 else (ip2, ip1))


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


class AS:
  metakeys = {'peering_data': 'peering_data_loaded'}

  @staticmethod
  def relationship(asn):
    return "as:%s:peering" % str(asn)

  @staticmethod
  def status(subkey):
    return "as:meta:%s" % AS.metakeys[subkey]
