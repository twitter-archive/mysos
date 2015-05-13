import os
import shutil
import tempfile
import unittest

from mysos.scheduler.scheduler import DEFAULT_TASK_CPUS, DEFAULT_TASK_MEM, DEFAULT_TASK_DISK
from mysos.scheduler.state import (
    LocalStateProvider,
    MySQLCluster,
    MySQLTask,
    Scheduler
)
from mysos.scheduler.password import gen_encryption_key, PasswordBox

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
    password_box = PasswordBox(gen_encryption_key())

    expected = MySQLCluster(
        'cluster1',
        'cluster_user',
        password_box.encrypt('cluster_password'),
        3,
        DEFAULT_TASK_CPUS,
        DEFAULT_TASK_MEM,
        DEFAULT_TASK_DISK)

    expected.tasks['task1'] = MySQLTask(
        'cluster1', 'task1', 'slave1', 'host1', 10000)

    self._state_provider.dump_cluster_state(expected)
    actual = self._state_provider.load_cluster_state('cluster1')

    assert expected.user == actual.user
    assert isinstance(actual.num_nodes, int)
    assert expected.num_nodes == actual.num_nodes
    assert len(expected.tasks) == len(actual.tasks)
    assert expected.tasks['task1'].port == actual.tasks['task1'].port
    assert expected.encrypted_password == actual.encrypted_password
    assert password_box.match('cluster_password', actual.encrypted_password)
