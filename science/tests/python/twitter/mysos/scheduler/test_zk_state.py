import cPickle
import os
import unittest

from twitter.mysos.scheduler.state import (
    MySQLCluster,
    MySQLTask,
    Scheduler,
    StateProvider
)
from twitter.mysos.scheduler.zk_state import ZooKeeperStateProvider

from kazoo.handlers.threading import SequentialThreadingHandler
from mesos.interface.mesos_pb2 import FrameworkInfo
import pytest
from zake.fake_client import FakeClient
from zake.fake_storage import FakeStorage


if 'MYSOS_DEBUG' in os.environ:
  from twitter.common import log
  from twitter.common.log.options import LogOptions
  LogOptions.set_stderr_log_level('google:DEBUG')
  LogOptions.set_simple(True)
  log.init('mysos_tests')


class TestZooKeeperStateProvider(unittest.TestCase):
  def setUp(self):
    self._storage = FakeStorage(SequentialThreadingHandler())
    self._client = FakeClient(storage=self._storage)
    self._client.start()
    self._state_provider = ZooKeeperStateProvider(self._client, '/mysos')

  def tearDown(self):
    self._client.stop()

  def test_scheduler_state(self):
    expected = Scheduler(FrameworkInfo(
        user='test_user',
        name='test_fw_name',
        checkpoint=True))
    expected.tasks = dict(taks1='cluster1', task2='cluster2')

    self._state_provider.dump_scheduler_state(expected)
    actual = self._state_provider.load_scheduler_state()

    assert expected.framework_info == actual.framework_info
    assert expected.tasks == actual.tasks

  def test_scheduler_state_errors(self):
    assert not self._state_provider.load_scheduler_state()  # Not an error for scheduler state to be
                                                            # not found.

    self._client.ensure_path("/mysos/state")
    self._client.create("/mysos/state/scheduler", cPickle.dumps(object()))
    with pytest.raises(StateProvider.Error):
      self._state_provider.load_scheduler_state()

  def test_cluster_state(self):
    expected = MySQLCluster('cluster1', 'cluster_user', 'cluster_password', 3)

    expected.tasks['task1'] = MySQLTask(
        'cluster1', 'task1', 'slave1', 'host1', 10000)

    self._state_provider.dump_cluster_state(expected)
    actual = self._state_provider.load_cluster_state('cluster1')

    assert expected.user == actual.user
    assert isinstance(actual.num_nodes, int)
    assert expected.num_nodes == actual.num_nodes
    assert len(expected.tasks) == len(actual.tasks)
    assert expected.tasks['task1'].port == actual.tasks['task1'].port

  def test_cluster_state_errors(self):
    with pytest.raises(StateProvider.Error):
      self._state_provider.load_cluster_state('nonexistent')

    self._client.ensure_path("/mysos/state/clusters")
    self._client.create("/mysos/state/cluster1", cPickle.dumps(object()))
    with pytest.raises(StateProvider.Error):
      self._state_provider.load_cluster_state('cluster1')
