"""

Used in combination with ProcessTraces. Creates a GraphML file from the data in
a local Redis database.

Utility to produce a GraphML file from traceroute data that has
been loaded into a local Redis database by inet_graph_process_data.

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

def __argparse__(subparser, parents):
  """ Add command line subparsers for this module
  to the subparser module provided to this function.

  This helps build part of a chain of commands.

  :subparser: an argparse.Subparser object
  :parents: A list of argparse.ArgumentParsers for which
            the intent is for them to be parents
  :returns: Nothing
  """


  pass
