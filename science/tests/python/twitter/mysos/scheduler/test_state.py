import os
import shutil
import tempfile
import unittest

from twitter.mysos.scheduler.state import (
    LocalStateProvider,
    MySQLCluster,
    MySQLTask,
    Scheduler
)

from mesos.interface.mesos_pb2 import FrameworkInfo


if 'MYSOS_DEBUG' in os.environ:
  from twitter.common import log
  from twitter.common.log.options import LogOptions
  LogOptions.set_stderr_log_level('google:DEBUG')
  LogOptions.set_simple(True)
  log.init('mysos_tests')


class TestState(unittest.TestCase):
  def setUp(self):
    self._tmpdir = tempfile.mkdtemp()
    self._state_provider = LocalStateProvider(self._tmpdir)

  def tearDown(self):
    shutil.rmtree(self._tmpdir, True)

  def test_scheduler_state(self):
    expected = Scheduler(FrameworkInfo(
        user='test_user',
        name='test_fw_name',
        checkpoint=True))
    expected.clusters.add('cluster2')
    expected.clusters.add('cluster1')

    self._state_provider.dump_scheduler_state(expected)
    actual = self._state_provider.load_scheduler_state()

    assert expected.framework_info == actual.framework_info
    assert expected.clusters == actual.clusters

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
