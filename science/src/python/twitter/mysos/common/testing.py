from collections import defaultdict


class Fake(object):
  """
    A fake object that does nothing but recording the method calls and arguments.
  """

  def __init__(self):
    self.method_calls = defaultdict(list)

  def __getattr__(self, attr):
    def enqueue_arguments(*args, **kw):
      self.method_calls[attr].append((args, kw))
    return enqueue_arguments
