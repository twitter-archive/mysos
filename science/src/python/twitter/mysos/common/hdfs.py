import os

from twitter.common import log
from twitter.common.concurrent import deadline, Timeout
from twitter.common.fs import HDFSHelper
from twitter.common.quantity import Amount, Data, Time

from .fetcher import Fetcher


HADOOP_CONF_DIR = '/etc/hadoop/conf'


class HDFSFetcher(Fetcher):
  """
    NOTE: Specify custom config directory using the environment variable 'HADOOP_CONF_DIR'.
  """

  def __init__(self, timeout=Amount(5, Time.MINUTES)):
    if not isinstance(timeout, Amount) or not isinstance(timeout.unit(), Time):
      raise ValueError("'timeout' must be an Amount of Time")
    self._timeout = timeout

  def fetch(self, uri, directory):
    log.info("Fetching %s from HDFS" % uri)

    if "JAVA_HOME" in os.environ:
      log.info("Using JAVA_HOME '%s' for HDFS commands" % os.environ["JAVA_HOME"])

    config = os.environ.get("HADOOP_CONF_DIR", HADOOP_CONF_DIR)
    h = HDFSHelper(config, heap_limit=Amount(256, Data.MB))
    try:
      f = lambda: h.copy_to_local(uri, directory)
      deadline(f, timeout=self._timeout, propagate=True, daemon=True)
    except HDFSHelper.InternalError as e:
      raise self.Error('Unable to fetch HDFS package: %s' % e)
    except Timeout as e:
      raise self.Error("Failed to fetch package from HDFS within : %s" % (self._timeout, e))
