import redis

import inettopology.util.decorators


@inettopology.util.decorators.singleton
class Redis:
  """ A Redis connection"""

  def __init__(self, redisinfo={'host': 'localhost', 'port': 6379, 'db': 0}):
    try:
      self._instance = redisinfo.instantiate()
    except AttributeError:
      self._instance = redis.StrictRedis(**redisinfo)

  def __call__(self, **kwargs):
    try:
      return self._instance
    except AttributeError:
      return Redis(**kwargs)
