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

from inettopology_popmap import lazy_load


class DataError(Exception):
  pass


def __argparse__(subparser, parents):
  """ Add the argparse cmdline arguments for data processing
  to the subparser """

  parser = subparser.add_parser('process',
                                formatter_class=argparse.RawTextHelpFormatter,
                                description=__doc__,
                                parents=parents)
  subparsers = parser.add_subparsers()

  # {parse} command
  parser_parse = subparsers.add_parser("parse",
                                       help="Parse Trace Routes. "
                                            "Perform ASN lookups for "
                                            "IPs as we go.",
                                       parents=parents)

  parser_parse.add_argument("--geoipdb",
                            help='MaxMind GeoIP Database to use'
                                 'for ASN lookups.',
                            required=True)

  parser_parse.add_argument("trace",
                            help="CAIDA trace file",
                            metavar="<trace file>")
  parser_parse.set_defaults(
      func=lazy_load('data.process', 'parse'),
      dump=False)

  parser_process_joins = subparsers.add_parser("process_joins",
                                               help="Process queued PoP joins",
                                               parents=parents)
  parser_process_joins.add_argument("--log_joins",
                                    type=str, metavar="LOG_FILE")
  parser_process_joins.set_defaults(
      func=lazy_load('data.process', 'process_delayed_joins'))

  parser_assign_pops = subparsers.add_parser(
      "assign_pops",
      help="Assign pops to the loaded links",
      parents=parents)

  parser_assign_pops.add_argument("--reset", action="store_true")

  parser_assign_pops.add_argument(
      "--process_failed",
      action="store_true",
      help="Process any links that were skipped in the initial run.")

  parser_assign_pops.set_defaults(
      func=lazy_load('data.process', 'assign_pops'))

  parser_cleanup = subparsers.add_parser(
      "cleanup",
      help="Remove all PoP related info from "
           "the database (but not the IP data)",
      parents=parents)

  parser_cleanup.add_argument(
      "--ip_links",
      help="Remove ip_links",
      action='store_true')

  parser_cleanup.set_defaults(
      func=lazy_load('cleanup', 'cleanup'))
