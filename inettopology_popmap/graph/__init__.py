"""
Used in combination with process. Creates a GraphML file from the data in
a local Redis database.

Utility to produce a GraphML file from traceroute data that has
been loaded into a local Redis database by 'inettopology popmap process'.

This script performs two actions sequentially:


1.  Loading the network graph from the Redis database and attaching Tor relays,
    clients and destinations to it based on the data provided.

    This step also performs some slight trimming of the graph by removing
    unconnected components, trimming degree-1 vertices (dangling edges of the
    tree), and collapsing degree-two nodes where it can be done without losing
    data.

    This intermediate graph representation is saved to disk as an GraphML file.
    This is important for repeatability, since this first step can take quite a
    while to perform.


2.  Reducing the size of the network graph. This step can load a previously
    created intermediate representation using the `--xml` option or it can take
    place immediately following step 1.

    At a high level, the network graph is reduced by removing all links that
    are not on a shortest path between two points of interest. In this case,
    points of interest are defined as Tor relays, clients, and destinations.

    If AS peering data is available, then each path is checked for
    valley-freeness, and if it is not valley-free, then a modified
    shortest-path algorithm is used to find the shortest valley-free path.

    The result of this process is written out as a GraphML file once complete.


Author: Chris Wacek
Email: cwacek@cs.georgetown.edu
Date: 02/22/202
"""

import argparse

import inettopology
import pkg_resources
from inettopology_popmap import lazy_load


def __argparse__(subparser, parents):
  """ Add command line subparsers for this module
  to the subparser module provided to this function.

  This helps build part of a chain of commands.

  :subparser: an argparse.Subparser object
  :parents: A list of argparse.ArgumentParsers for which
            the intent is for them to be parents
  :returns: Nothing
  """

  parser = subparser.add_parser('graph',
                                formatter_class=argparse.RawTextHelpFormatter,
                                description=__doc__,
                                parents=parents)

  sub = parser.add_subparsers()

  # {cleanup}
  cleanup_parser = sub.add_parser(
      "cleanup",
      help="Cleanup any extraneous keys in the database")

  cleanup_parser.set_defaults(func=lazy_load('graph.cleanup', 'cleanup'))

  # {create}
  create_parser = sub.add_parser(
      "create",
      help="Create a graph")

  # [save|reload]
  group = create_parser.add_mutually_exclusive_group(required=True)
  group.add_argument(
      "--reload",
      type=str,
      help="Reload the data from redis. Store the "
           "GraphML intermediary representation in FILENAME",
      metavar="FILENAME")
  group.add_argument(
      "--xml",
      type=str,
      help="Load from the GraphML file FILENAME",
      metavar="FILENAME")

  create_parser.add_argument(
      '--save',
      type=str,
      help="Save output with this prefix",
      metavar="PREFIX",
      required=True)

  create_parser.add_argument(
      "-c",
      "--num_clients",
      help="The number of clients to attach",
      type=int,
      dest="num_clients")

  create_parser.add_argument("--client_data",
                             help="File containing client data",
                             metavar="CLIENT_DATAFILE")

  create_parser.add_argument(
      "-d", "--num_dests",
      help="The number of destinations to "
           "attach. Destinations are drawn from the IP addresses "
           "used by the Alexa top 10000 sites",
      type=int,
      dest="num_dests")

  create_parser.add_argument(
      "--tor_relays",
      help="A JSON file containing Tor relays to use as a list "
           "of objects. Each object should at the minimum contain "
           "the following keys: `relay_ip`, `pop` (the attach point), "
           " and `asn`. A document of this type can be created using "
           "{0}.".format(
               pkg_resources.resource_filename(
                   'inettopology_popmap.resources',
                   'ruby_ip_matcher')),
      required=True,
      metavar="RELAY_FILE")

  create_parser.add_argument(
      "--log",
      help="Log output here. Defaults to <save_opt>.log",
      metavar='<log file>')

  create_parser.set_defaults(
      func=lazy_load('graph.core', 'create_graph', check_create_args))


def check_create_args(args):
  """ Check the result of argument parsing to find any issues.

  :args: argparse.Namespace containing parsed arguments
  """

  import logging
  thislogger = logging.getLogger(__name__)

  logfile = args.log if args.log else '{0}.log'.format(args.save)
  fh = logging.FileHandler(filename=logfile)
  fh.setLevel(logging.DEBUG)
  formatter = logging.Formatter(
      '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  fh.setFormatter(formatter)

  thislogger.addHandler(fh)

  def all_if_one(argobj, *required):
    argsexist = [vars(argobj)[x] for x in required]
    if any(argsexist) and not all(argsexist):
      return False
    return True

  if not all_if_one(args, "client_data", "num_clients"):
    thislogger.error("'client_data' and 'num_clients' must be given together")
    raise inettopology.SilentExit()
