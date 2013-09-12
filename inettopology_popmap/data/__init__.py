"""
===== ProcessTraces =====

Utility to process traceroute data from CAIDA
into a set of data that can be turned into an
internet topology.

*** This tool uses Redis as a backend, and currently
*** simply defaults to db 0 on the standard port.
*** You may want to adjust this if that's not right
*** for you. Also if Redis isn't running, that's
*** obviously an issue.

Note: The trace route files are expected in the form output by the
sc_warts2text command contained in the scamper package (not included).

Order of Commands
-----------------
1. load_IP_data  -  There must be at least an 'asn'
                    key for every IP in the traceroutes.
                    To obtain a list of the IPs in
                    the traceroutes, try 'dump_ips'.

2. parse         -  This can be parallelized (i.e. you
                    can many of these processes at once).

3. assign_pops   -  This can also be parallelized, with
                    one caveat. If several are run
                    simultaneously, this command must be
                    run again (only one instance), with
                    the '--process_failed' flag.

                    If for any reason you need to reprocess
                    all of the pop assignments, use the
                    '--reset' flag.

4. process_joins  - (no extra notes)
"""

import argparse
import importlib


def lazy_load(package, function):
  """ A helper to lazy load functions that start
  actions to avoid circular references.
  """
  def runner(args):
    module = importlib.import_module("{0}.{1}".format(
                                     __name__, package))
    import pdb; pdb.set_trace()
    module.__dict__[function](args)

  return runner


def __argparse__(subparser, parents):
  """ Add the argparse cmdline arguments for data processing
  to the subparser """

  parser = subparser.add_parser('process',
                                formatter_class=argparse.RawTextHelpFormatter,
                                description=__doc__)
  subparsers = parser.add_subparsers()

  # {parse} command
  parser_parse = subparsers.add_parser("parse",
                                       help="Parse Trace Routes")
  parser_parse.add_argument("trace",
                            help="CAIDA trace file",
                            metavar="<trace file>")
  parser_parse.set_defaults(
      func=lazy_load('process', 'parse'),
      dump=False)

  # {dump_ips} command
  parser_dump_ips = subparsers.add_parser(
      "dump_ips",
      help="Dump all IPs from a traceroute file")

  parser_dump_ips.add_argument("trace",
                               help="CAIDA trace file",
                               metavar="<trace file>")
  parser_dump_ips.set_defaults(
      func=lazy_load('process', 'parse'),
      dump=True)

  parser_process_joins = subparsers.add_parser("process_joins",
                                               help="Process queued PoP joins")
  parser_process_joins.add_argument("--log_joins",
                                    type=str, metavar="LOG_FILE")
  parser_process_joins.set_defaults(
      func=lazy_load('process', 'process_delayed_joins'))

  parser_load_asn = subparsers.add_parser(
      "load_IP_data",
      formatter_class=argparse.RawTextHelpFormatter,
      help=("Load IP attributes from a file. "
            "Will not set the 'pop' attribute."))
  parser_load_asn.add_argument("attr_file",
                               help=("""\
              Attribute file in the form:
                  <ip> <key> <value> <key2> <value2> ...
                  <ip> <key> <value> <key2> <value2> ...
              OR the form:
                  # <key> <key2> ... <keyN>
                  <ip> <value> <value1> ... <valueN>
                  <ip> <value> <value1> ... <valueN>
                  """))
  parser_load_asn.set_defaults(
      func=lazy_load('process', 'load_attr_data'))

  parser_assign_pops = subparsers.add_parser(
      "assign_pops",
      help="Assign pops to the loaded links")

  parser_assign_pops.add_argument("--reset", action="store_true")

  parser_assign_pops.add_argument(
      "--process_failed",
      action="store_true",
      help="Process any links that were skipped in the initial run.")

  parser_assign_pops.set_defaults(
      func=lazy_load('process', 'assign_pops'))

  parser_cleanup = subparsers.add_parser(
      "cleanup",
      help="Remove all PoP related info from "
           "the database (but not the IP data)")

  parser_cleanup.add_argument(
      "--ip_links",
      help="Remove ip_links",
      action='store_true')

  parser_cleanup.set_defaults(
      func=lazy_load('process', 'cleanup'))
