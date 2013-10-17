
class EmptyListError(Exception):
    pass


class ASNNotKnown(Exception):
    pass


class DuplicateVertex(Exception):
    pass


class VertexList(dict):
    def __init__(self):
      self._available_attrs = set()

    def nx_tuple_iter(self):
      for node in self:
        yield (node, self[node])

    def attrs_for(self, vid):
      """ Return the attribute dictionary for vertex 'vid'. """
      return self[vid]

    def write(self, f):
        for vertex, attrdict in self.iteritems():
            f.write("%s " % (vertex))
            for attr, val in attrdict.iteritems():
                f.write("%s=%s " % (attr, val))
            f.write("\n")

    def add_vertex(self, vid, **kwargs):
        if vid in self:
            raise DuplicateVertex("%s" % vid)
        self[vid] = kwargs
        self._available_attrs |= set(kwargs.keys())

    def get_by_attr(self, attr, *types):
        """
        Return the a list of IDs of vertexes where the
        value of <b>attr</b> is in <b>types</b>.
        """
        if attr not in self._available_attrs:
          return []
        retlist = []
        for vid, attrs in self.iteritems():
          try:
            if vid[attrs] in types:
              retlist.append(vid)
          except KeyError:
            #It's okay to not find one, we just won't return it.
            pass
        return retlist


class EdgeLink(object):
  def __init__(self, end1, end2, attrs=dict()):
    self.pair = (end1, end2)
    self.attrs = attrs

  def add_attribute(self, name, value):
    self.attrs[name] = value

  def nx_tuple(self):
    return (self.pair[0], self.pair[1], self.attrs)


class Stats(dict):
    def __init__(self, keys):
        self.__keytypes = dict()
        for key, keytype in keys.iteritems():
            if isinstance(keytype, basestring):
                self[key] = ''
                self.__keytypes[key] = basestring
            elif keytype == set:
                self[key] = set()
                self.__keytypes[key] = set
            elif keytype == dict:
                self[key] = dict()
                self.__keytypes[key] = dict
            else:
                self[key] = 0
                self.__keytypes[key] = int

    def incr(self, key, val=None):
        if val:
            try:
                self[key].add(val)
            except TypeError:
                self[key] += val
        else:
            self[key] += 1
