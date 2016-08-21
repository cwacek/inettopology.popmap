"""
Author: Chris Wacek
Email: cwacek@cs.georgetown.edu
Date: 02/22/202
"""
import re
import inettopology.util as util


class ParseError(Exception):
  pass


class EmptyTraceError(ParseError):
  pass


class TraceParser(object):

  ipregex = re.compile("[1-9][0-9]{0,2}(\.[0-9]{1,3}){3}")
  privateipregex = re.compile("(^127\.0\.0\.1)|(^192\.168)|(^10\.)|(^172\.1[6-9])|(^172\.2[0-9])|(^172\.3[0-1])")

  @classmethod
  def parse(cls, tracelines):
    if len(tracelines) == 0:
      raise EmptyTraceError("No lines to trace")
    raw = cls.transform_raw_data(tracelines)
    return cls.parsepairs(raw)

  @classmethod
  def ip_is_valid(cls, ip):
    if cls.privateipregex.match(ip) or not cls.ipregex.match(ip):
      return False
    return True

  @classmethod
  def transform_raw_data(cls, tracelines):
    """
    Transform a raw trace of the form:

    # traceroute from 129.186.1.240 to 184.66.242.2
    # 1  129.186.6.251  0.235 ms
    # 2  129.186.254.131  0.787 ms
    # 3  192.245.179.52  0.290 ms
    # 4  192.245.179.166  0.318 ms

    into a list of tuples of the form (ip, total_delay).
    """
    transformed = []
    for i, line in enumerate(tracelines):
      fields = line.split()
      if i == 0:
        try:
          if not cls.ip_is_valid(fields[2]):
            raise ParseError("Invalid IP in first line of trace: [{0}]"
                             .format(line))
          transformed.append([fields[2], 0.0])
        except IndexError:
          if cls.privateipregex.match(fields[2]):
            continue
          raise ParseError("No IP found on first line of trace: [%s]"
                           .format(line))

      else:
        try:
          if not cls.ip_is_valid(fields[1]):
            raise ParseError()
          transformed.append([fields[1], float(fields[2])])
        except (IndexError, ParseError):
          """ If we encounter errors here, it's probably because there are *
          fields.  Our response to those will simply be to skip them - if we
          know the absolute distance, then the link 'exists'. """
          continue

    return transformed

  @classmethod
  def parsepairs(cls, rawdata):
    """
    Parse the raw data belonging to this instance and turn
    them into pairs of IP addresses with a corresponding delay.

    """
    removed = None
    if rawdata[-1][1] > 800:
      removed = rawdata[-1]
      rawdata = rawdata[:-1]

    cls.sane_itize(rawdata)

    pairs = []

    for latest, previous in util.pairwise(reversed(rawdata)):
      dist = round(latest[1] - previous[1], 3)
			if dist == 0.0:
				dist = 1.0 
      if previous[0] != latest[0]:
        pairs.insert(0, (previous[0], latest[0], dist))

    return (pairs, removed)

  @classmethod
  def sane_itize(cls, rawdata):
    """
    Take the raw data, and for any case where it goes A->B->C
    with delay(B) > delay(C) && delay(A) < delay(B), set delay(B)
    to half the distance between A and C. For any case where
    delay(B) > delay(C) && delay(A) > delay(C), set delay(B) equal
    to delay(C).
    """
    for latest, previous, previous2 in util.triwise(reversed(rawdata)):
      maxval = latest[1]
      if previous[1] > maxval:
        if previous2[1] > maxval:  # B > C; A > C
          previous[1] = maxval
        else:  # B > C, B > A
          previous[1] = (maxval + previous2[1]) / 2


def different_as(r, ip1, ip2, ignore=False):
    """
    Return True if r().hget(ip1,'asn') != r().hget(ip2,'asn').
    Else False
    Return None if either side of the link has "N/A" for it's
    ASN
    """
    if ignore:
        return False

    ip1_asn = r.hget(ip1, 'asn')
    ip2_asn = r.hget(ip2, 'asn')

    if not ip1_asn or not ip2_asn:
      return None

    return (ip1_asn != ip2_asn)


def different_24(ip1, ip2, ignore=False):
    """
    Return true if ip1 and ip2 are in different /24s
    Else false
    """
    if ignore:
        return False
    ip1_24 = ip1.split('.')[3]
    ip2_24 = ip2.split('.')[3]
    return True if ip1_24 != ip2_24 else False
