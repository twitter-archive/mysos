import Queue
import os
import unittest

from twitter.common.quantity import Amount, Time
from twitter.mysos.common.testing import Fake
from twitter.mysos.scheduler.elector import MySQLMasterElector


if 'MYSOS_DEBUG' in os.environ:
  from twitter.common import log
  from twitter.common.log.options import LogOptions
  LogOptions.set_stderr_log_level('google:DEBUG')
  LogOptions.set_simple(True)
  log.init('mysos_tests')


class FakeDriver(Fake): pass


class TestElector(unittest.TestCase):
  def setUp(self):
    self._driver = FakeDriver()
    self._epoch = 1
    self._result = Queue.Queue()
    self._elector = MySQLMasterElector(
        self._driver,
        "cluster0",
        self._epoch,
        lambda x: self._result.put(x),
        Amount(1, Time.SECONDS),
        Amount(150, Time.MILLISECONDS))
    self._elector.start()

  def tearDown(self):
    if self._elector:  # Terminate the elector if it's not used in the test.
      self._elector.abort()
      self._elector.join()

  def test_single_slave(self):
    slave1 = ("task_id1", "slave_id1")
    self._elector.add_slave(*slave1)
    self._elector.update_position(self._epoch, slave1[0], 1)

    assert self._result.get(True, 1) == slave1[0]

  def test_two_slaves(self):
    slave1 = ("task_id1", "slave_id1")
    slave2 = ("task_id2", "slave_id2")
    self._elector.add_slave(*slave1)
    self._elector.add_slave(*slave2)

    self._elector.update_position(self._epoch, slave1[0], 1)
    self._elector.update_position(self._epoch, slave2[0], 2)

    assert self._result.get(True, 1) == slave2[0]

  def test_two_slaves_complex_position(self):
    slave1 = ("task_id1", "slave_id1")
    slave2 = ("task_id2", "slave_id2")
    self._elector.add_slave(*slave1)
    self._elector.add_slave(*slave2)

    # The positions are sequences of numeric strings.
    self._elector.update_position(self._epoch, slave1[0], ["1", "2"])
    self._elector.update_position(self._epoch, slave2[0], ["2", "1"])

    assert self._result.get(True, 1) == slave2[0]

  def test_delayed_update(self):
    slave1 = ("task_id1", "slave_id1")
    slave2 = ("task_id2", "slave_id2")
    self._elector.add_slave(*slave1)
    self._elector.add_slave(*slave2)
    self._elector.update_position(self._epoch, slave2[0], 2)

    # Force an election (after timing out) and test that slave2 is elected because it's the only
    # slave that responded.
    self._elector._elect(timedout=True)
    assert self._result.get(True, 1) == slave2[0]

  def test_position_for_invalid_slave(self):
    slave1 = ("task_id1", "slave_id1")
    slave2 = ("task_id2", "slave_id2")
    self._elector.update_position(self._epoch, slave1[0], 100)  # This update is ignored.
    self._elector.add_slave(*slave1)
    self._elector.add_slave(*slave2)
    self._elector.update_position(self._epoch, slave2[0], 1)

    # Timeout is 1 second. Testing the organic 'timeout' of the thread.
    assert self._result.get(True, 2) == slave2[0]

  def test_position_for_previous_epoch(self):
    slave1 = ("task_id1", "slave_id1")
    slave2 = ("task_id2", "slave_id2")
    self._elector.add_slave(*slave1)
    self._elector.add_slave(*slave2)

    self._elector.update_position(self._epoch - 1, slave1[0], 100)  # Update from a previous epoch.
    self._elector.update_position(self._epoch, slave2[0], 1)

    # Induce an election after it timed out.
    self._elector._elect(timedout=True)
    assert self._result.get(True, 1) == slave2[0]

  def test_remove_slave_after_election(self):
    slave1 = ("task_id1", "slave_id1")
    slave2 = ("task_id2", "slave_id2")
    self._elector.add_slave(*slave1)
    self._elector.add_slave(*slave2)

    self._elector.update_position(self._epoch, slave1[0], 1)
    self._elector.update_position(self._epoch, slave2[0], 2)

    assert self._result.get(True, 1) == slave2[0]

    # At this point a master is already elected. Slave removal is ignored.
    self._elector.remove_slave(slave2[0])

    assert len(self._elector._positions) == 2

  def test_remove_slave_during_election(self):
    slave1 = ("task_id1", "slave_id1")
    slave2 = ("task_id2", "slave_id2")
    self._elector.add_slave(*slave1)
    self._elector.add_slave(*slave2)

    self._elector.update_position(self._epoch, slave2[0], 2)

    # Election still ongoing. Removing slave2 allows slave1 to be elected.
    self._elector.remove_slave(slave2[0])
    self._elector.update_position(self._epoch, slave1[0], 1)

    self._elector._elect()
    assert self._result.get(True, 1) == slave1[0]
