import importlib


def lazy_load(package, function, check_args=None):
  """ A helper to lazy load functions that start
  actions to avoid circular references. If :check_args:
  is given, run that to allow checking arguments
  specific to certain functions.
  """
  def runner(args):

    if check_args is not None:
      check_args(args)

    module = importlib.import_module("{0}.{1}".format(
                                     __name__, package))
    module.__dict__[function](args)
