import heapq

class pqueue(object):

  def __init__(self):
    self.queue = []

  def push(self,value,priority):
    heapq.heappush(self.queue,(priority,self.queue))
    return len(self.queue)

  def pop(self):
    prio,val = heapq.heappop(self.queue)
    return val

  def peek(self):
    return self.queue[0]

  def __len__(self):
    return len(self.queue)

  def __repr__(self):
    return repr(self.q)

  def empty(self):
    return len(self.queue) == 0


