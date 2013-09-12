import redis

import inettopology.util.decorators


@inettopology.util.decorators.singleton
class Redis:
  """ A Redis connection"""

  def __init__(self):
    pass

  def config_manual(self, host="localhost", port=6379, db=0):
    self._instance = redis.StrictRedis(host=host, port=port, db=db)

  def config(self, redisinfo):
    self._instance = redisinfo.instantiate()

  def __call__(self):
    return self._instance
