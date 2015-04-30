import functools
import inspect

from twitter.common import log


def logged(func):
  arg_names = inspect.getargspec(func).args[1:]
  @functools.wraps(func)
  def wrapped_func(self, *args):
    log.debug('%s(%s)' % (
      func.__name__,
      ', '.join('%s=%s' % (name, arg) for (name, arg) in zip(arg_names, args))))
    return func(self, *args)
  return wrapped_func


def synchronized(func):
  @functools.wraps(func)
  def synchronizer(self, *args, **kwargs):
    assert hasattr(self, '_lock'), "Need to define a _lock to use this decorator"
    with self._lock:
      return func(self, *args, **kwargs)
  return synchronizer
