class EmptyListError(Exception):
    pass


def decile_transform(input_list):
  sorted_list = sorted(map(float, input_list))

  deciles = [0 for x in xrange(10)]
  interval = len(sorted_list) / 10.0

  if len(sorted_list) == 0:
    raise EmptyListError()
  else:
    for decile in xrange(10):
      deciles[decile] = sorted_list[int(decile * interval)]

  return deciles
