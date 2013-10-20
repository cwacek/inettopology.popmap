import redis

import inettopology.util.decorators


@inettopology.util.decorators.singleton
class Redis:
  """ A Redis connection"""

  def __init__(self, *redisinfo, **conninfo):
    try:
      self._instance = redisinfo[0].instantiate()
    except (AttributeError, IndexError):
      self._instance = redis.StrictRedis(**conninfo)

  def __call__(self, **kwargs):
    try:
      return self._instance
    except AttributeError:
      return Redis(**kwargs)
