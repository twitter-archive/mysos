from abc import abstractmethod

from twitter.common.lang import Interface


class Fetcher(Interface):
  class Error(Exception): pass

  @abstractmethod
  def fetch(self, uri, directory):
    pass


class FetcherFactory(object):
  """A singleton factory for Fetchers."""

  _FETCHERS = {}

  @classmethod
  def register_fetcher(cls, scheme, fetcher):
    cls._FETCHERS[scheme.rstrip('://')] = fetcher

  @classmethod
  def get_fetcher(cls, uri):
    """
      :return: A Fetcher instance that matches this URI. None if no fetcher is registered with the
               URI's scheme.
    """
    scheme = uri.split('://')[0]
    fetcher = cls._FETCHERS.get(scheme)
    if not fetcher:
      raise ValueError("No Fetcher is registered for URI scheme '%s'" % scheme)

    return fetcher
