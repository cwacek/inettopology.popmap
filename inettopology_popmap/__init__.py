import importlib


def lazy_load(package, function):
  """ A helper to lazy load functions that start
  actions to avoid circular references.
  """
  def runner(args):
    module = importlib.import_module("{0}.{1}".format(
                                     __name__, package))
    module.__dict__[function](args)

