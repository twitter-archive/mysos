from collections import OrderedDict
from datetime import datetime, timedelta
import threading

from twitter.common import log
from twitter.common.exceptions import ExceptionalThread
from twitter.common.quantity import Amount, Time

import mesos.interface.mesos_pb2 as mesos_pb2


class MySQLMasterElector(ExceptionalThread):
  """
    Elect the most current slave in the cluster to be the new master.

    The current election algorithm:
      The elector queries slaves periodically (to tolerate message loss) until all slaves have
      responded with their current positions and the slave with the highest (most current)
      position is elected the new master. If the election timeout has passed and not all slaves have
      responded, the master is elected from the ones that have.

    Thread-safety:
      The elector runs as a separate thread and it periodically queries the slaves util the election
      completes or is aborted. Its public methods are thread-safe.

    Usage:
      After constructing the elector, add all currently known candidates of the election through
      'elector.add_slave()' before calling 'elector.start()' to make sure it waits for all of them.

    NOTE: An elector is not reusable. To restart the election, create another MySQLMasterElector
    instance.
  """

  def __init__(
      self,
      driver,
      cluster_name,
      epoch,
      master_callback,
      election_timeout,
      query_interval=Amount(1, Time.SECONDS)):
    """
      :param driver: The SchedulerDriver for querying the slaves.
      :param cluster_name: The name of the MySQL cluster.
      :param epoch: The master epoch that identifies this election.
      :param master_callback: The callback function with one argument: the 'task_id' of the elected
                              master which could be None if no one is electable.
      :param election_timeout: The amount of time the elector waits for all slaves to respond. If
                               not all slaves have responded within the timeout, then the master is
                               elected from the ones who have.
      :param query_interval: The timeout before the elector re-sends queries for positions.

      :type epoch: int
      :type query_interval: Amount
      :type election_timeout: Amount
      :type master_callback: function
    """
    super(MySQLMasterElector, self).__init__()

    if not isinstance(epoch, int):
      raise TypeError("'epoch' should be an int")
    if not isinstance(query_interval, Amount) or not isinstance(query_interval.unit(), Time):
      raise ValueError("'query_interval' must be an Amount of Time")
    if not isinstance(election_timeout, Amount) or not isinstance(election_timeout.unit(), Time):
      raise ValueError("'election_timeout' must be an Amount of Time")
    if not hasattr(master_callback, '__call__'):
      raise TypeError("master_callback must be a function")

    self._query_interval = query_interval.as_(Time.SECONDS)

    self._election_deadline = (
        datetime.utcnow() + timedelta(seconds=election_timeout.as_(Time.SECONDS)))

    self._driver = driver
    self._cluster_name = cluster_name  # For logging.
    self._epoch = epoch
    self._master_callback = master_callback

    self._positions = OrderedDict()  # Slave {Task ID: Position} mappings. Use OrderedDict so we can
                                     # easily locate the first added slave.
    self._mesos_slaves = {}  # Slave {Task ID, Mesos slave ID)} mappings.
    self._master = None  # Elected master (its TaskID); initially None and can still be None after
                         # the election has timed out and there are no slaves to elect from.

    self._lock = threading.Lock()
    self._aborted = threading.Event()  # Elector thread aborted (don't invoke callback).
    self._completed = threading.Event()  # Election process completed (invoke callback).

  def add_slave(self, task_id, mesos_slave_id):
    """
      Add a new MySQL slave (Task ID, Mesos Slave ID) which could affect the ongoing election.
    """
    with self._lock:
      if self._completed.is_set():
        log.debug("Ignoring addition of slave %s because the election has completed" % task_id)
        return

      log.info('Adding slave %s to the election for cluster %s' % (
          str((task_id, mesos_slave_id)),
          self._cluster_name))

      self._positions[task_id] = None
      self._mesos_slaves[task_id] = mesos_slave_id

  def remove_slave(self, task_id):
    """
      Remove a slave from the election process.
    """
    with self._lock:
      if self._completed.is_set():
        log.debug("Ignoring removal of slave %s because the election has completed" % task_id)
        return

      log.info('Removing slave %s from election for cluster %s' % (
          str((task_id, self._mesos_slaves[task_id])), self._cluster_name))

      assert task_id in self._positions
      assert task_id in self._mesos_slaves
      del self._positions[task_id]
      del self._mesos_slaves[task_id]

  def update_position(self, epoch, task_id, position):
    """
      Called by the launcher upon receiving the executor's response to position query.

      :type epoch: int
      :param epoch: The master epoch this position is for.
      :param position: The position of the slave's log (not necessarily numeric). The elector
                       doesn't care about its exact format as long as it is comparable.
    """
    if not isinstance(epoch, int):
      raise TypeError("'epoch' should be an int")

    with self._lock:
      if self._completed.is_set():
        log.debug("Ignoring position %s from slave %s because the election has completed" % (
            position, task_id))
        return

      if epoch != self._epoch:
        log.info(
            "Ignoring position %s from slave %s due to epoch mismatch: (expected: %s, actual: %s)" %
            (position, task_id, self._epoch, epoch))
        return

      if task_id not in self._mesos_slaves:
        log.warn("Ignoring unsolicited position response %s from MySQL slave %s" % (
            str(position), task_id))
        return

      log.info('Updating position to %s for slave %s of cluster %s' % (
          str(position),
          str((task_id, self._mesos_slaves[task_id])),
          self._cluster_name))

      self._positions[task_id] = position

  def run(self):
    # Re-run the election in a loop periodically until a master can be elected or the elector is
    # aborted.
    while not self._aborted.is_set() and not self._completed.wait(self._query_interval):
      if datetime.utcnow() < self._election_deadline:
        self._elect(timedout=False)
      else:
        log.info("Timed out waiting for all slaves to respond. Now elect from existing responses")
        self._elect(timedout=True)
        if not self._completed.is_set():
          log.warn("No slave is electable after timeout")

    if self._aborted.is_set():  # If asked to stop, directly return without triggering the callback.
      log.info("Asked to stop the elector thread. Stopping...")
      return

    self._master_callback(self._master)  # Invoke the callback from the elector thread.
    log.info("Stopping the elector thread for cluster %s because the election has completed" %
        self._cluster_name)

  def _elect(self, timedout=False):
    """
      Try to elect the master from MySQL slaves.

      Elect the slave with the highest position if all slaves have responded, otherwise re-query the
      remaining slaves for log positions (unless 'timedout' is True).

      :param timedout: If True, just elect the slave with the highest position from the ones who
                       have responded.

      NOTE: If 'timedout' is True and no slaves have responded (but there are running slaves in
      the cluster), theoretically we can still randomly elect one from them but currently we don't
      as this would suggest some major issues with the MySQL or Mesos cluster and it's dangerous to
      do so.
    """
    with self._lock:
      # Special-casing the first epoch because this is when the cluster first starts and every
      # slave should be the same: just pick the first slave that comes up.
      if self._epoch == 0 and len(self._positions) > 0:
        self._master = next(iter(self._positions))
        self._completed.set()
        return

      if timedout or (self._positions and _all(self._positions.values())):
        # Pick the slave with the highest position (value of the key-value pair). If all items have
        # the same value, an arbitrary (but deterministic) one is chosen.
        if _any(self._positions.values()):  # Need at least one position.
          master_task, _ = max(self._positions.items(), key=lambda kv: kv[1])
          log.info('Elected master %s for cluster %s' % (master_task, self._cluster_name))
          self._master = master_task
          self._completed.set()
      else:
        # (Re)query the remaining slaves for log positions.
        for task_id in self._positions:
          if not self._positions[task_id]:
            self._query_slave(task_id)

  def _query_slave(self, task_id):
    assert task_id in self._mesos_slaves

    log.info('Querying MySQL slave %s for its log position' % str(
        (task_id, self._mesos_slaves[task_id])))

    # Because the elector re-sends messages, it's necessary to use the epoch to differentiate
    # responses for each election.
    self._driver.sendFrameworkMessage(
        mesos_pb2.ExecutorID(value=task_id),
        mesos_pb2.SlaveID(value=self._mesos_slaves[task_id]),
        str(self._epoch))  # Send the slave the epoch so it can be included in the response.

  def abort(self):
    """Stop the elector thread."""
    self._aborted.set()


def _all(iterable):
  """Same as built-in version of all() except it explicitly checks if an element "is None"."""
  for element in iterable:
    if element is None:
      return False
  return True


def _any(iterable):
  """Same as built-in version of any() except it explicitly checks if an element "is not None"."""
  for element in iterable:
    if element is not None:
      return True
  return False
