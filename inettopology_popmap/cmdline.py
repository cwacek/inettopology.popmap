import argparse

import logging
logger = logging.getLogger(__name__)

import inettopology_popmap.data
import inettopology.util.structures as structures


def __argparse__(subparser, parents=[]):
  """Add parsers for this module to the passed
  in subparser. Include :parents: as parent
  parsers

  :subparser: argparse.Subparser()
  :parents: List of argparse.ArgumentParsers()
  :returns: None
  """

  gen_p = argparse.ArgumentParser(add_help=False)
  gen_p.add_argument("--redis", action=structures.RedisArgAction,
                     default={'host': 'localhost', 'port': 6379, 'db': 0},
                     help="Redis connection info for router server "
                          "(default: 'localhost:6379:0')")
  gen_p.add_argument("-v", "--verbose", action='count', default=0)

  parents.append(gen_p)
  inettopology_popmap.data.__argparse__(subparser, parents)
