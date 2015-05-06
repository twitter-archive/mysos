import os
import signal
import unittest

from mysos.common.cluster import ClusterManager
from mysos.common.testing import Fake
from mysos.executor.noop_installer import NoopPackageInstaller
from mysos.executor.mysos_task_runner import MysosTaskRunner
from mysos.executor.task_runner import TaskError
from mysos.executor.testing.fake import FakeTaskControl

from kazoo.handlers.threading import SequentialThreadingHandler
import pytest
from twitter.common.concurrent import deadline
from twitter.common.quantity import Amount, Time
from twitter.common.zookeeper.serverset.endpoint import Endpoint, ServiceInstance
from zake.fake_client import FakeClient
from zake.fake_storage import FakeStorage


if 'MYSOS_DEBUG' in os.environ:
  from twitter.common import log
  from twitter.common.log.options import LogOptions
  LogOptions.set_stderr_log_level('google:DEBUG')
  LogOptions.set_simple(True)
  log.init('mysos_tests')


class FakeStateManager(Fake): pass


class TestTaskRunner(unittest.TestCase):
  def setUp(self):
    self._storage = FakeStorage(SequentialThreadingHandler())
    self._client = FakeClient(storage=self._storage)
    self._client.start()
    self._self_instance = ServiceInstance(Endpoint("host", 10000))
    self._state_manager = FakeStateManager()

  def tearDown(self):
    self._client.stop()

  def test_stop(self):
    task_control = FakeTaskControl()
    runner = MysosTaskRunner(
        self._self_instance,
        self._client,
        "/home/test/my_cluster",
        NoopPackageInstaller(),
        task_control,
        self._state_manager)
    runner.start()
    assert runner.stop()

    # Killed by SIGTERM.
    assert deadline(runner.join, Amount(1, Time.SECONDS)) == -signal.SIGTERM

  def test_demote(self):
    task_control = FakeTaskControl()
    runner = MysosTaskRunner(
        self._self_instance,
        self._client,
        "/home/test/my_cluster",
        NoopPackageInstaller(),
        task_control,
        self._state_manager)

    manager = ClusterManager(self._client, "/home/test/my_cluster")
    runner.start()

    self_member = manager.add_member(self._self_instance)

    # 'self_instance' becomes the master.
    manager.promote_member(self_member)

    runner.promoted.wait(1)

    another_member = manager.add_member(ServiceInstance(Endpoint("another_host", 10000)))

    # This demotes 'self_instance', which should cause runner to stop.
    manager.promote_member(another_member)

    assert deadline(runner.join, Amount(1, Time.SECONDS))

  def test_reparent(self):
    task_control = FakeTaskControl()
    runner = MysosTaskRunner(
        self._self_instance,
        self._client,
        "/home/test/my_cluster",
        NoopPackageInstaller(),
        task_control,
        self._state_manager)

    manager = ClusterManager(self._client, "/home/test/my_cluster")
    runner.start()

    # Promote another instance.
    master = ServiceInstance(Endpoint("another_host", 10000))
    another_member = manager.add_member(master)
    manager.promote_member(another_member)

    assert runner.master.get(True, 1) == master

    assert runner.stop()
    assert deadline(runner.join, Amount(1, Time.SECONDS))

  def test_mysqld_error(self):
    task_control = FakeTaskControl(mysqld="exit 123")
    runner = MysosTaskRunner(
        self._self_instance,
        self._client,
        "/home/test/my_cluster",
        NoopPackageInstaller(),
        task_control,
        self._state_manager)

    runner.start()
    assert deadline(runner.join, Amount(1, Time.SECONDS)) == 123

  def test_start_command_error(self):
    task_control = FakeTaskControl(start_cmd="exit 1")
    runner = MysosTaskRunner(
        self._self_instance,
        self._client,
        "/home/test/my_cluster",
        NoopPackageInstaller(),
        task_control,
        self._state_manager)

    with pytest.raises(TaskError) as e:
      runner.start()
    assert e.value.message.startswith("Failed to start MySQL task")

  def test_promote_command_error(self):
    task_control = FakeTaskControl(promote_cmd="exit 1")
    runner = MysosTaskRunner(
        self._self_instance,
        self._client,
        "/home/test/my_cluster",
        NoopPackageInstaller(),
        task_control,
        self._state_manager)

    manager = ClusterManager(self._client, "/home/test/my_cluster")
    runner.start()

    self_member = manager.add_member(self._self_instance)

    # 'self_instance' becomes the master.
    manager.promote_member(self_member)

    runner.promoted.wait(1)

    with pytest.raises(TaskError) as e:
      runner.join()
    assert e.value.message.startswith("Failed to promote the slave")

  def test_get_log_position(self):
    task_control = FakeTaskControl(position=1)
    runner = MysosTaskRunner(
        self._self_instance,
        self._client,
        "/home/test/my_cluster",
        NoopPackageInstaller(),
        task_control,
        self._state_manager)

    runner.start()
    assert runner.get_log_position() == 1

  def test_get_log_position_error(self):
    task_control = FakeTaskControl(get_log_position_cmd="exit 1")
    runner = MysosTaskRunner(
        self._self_instance,
        self._client,
        "/home/test/my_cluster",
        NoopPackageInstaller(),
        task_control,
        self._state_manager)

    with pytest.raises(TaskError) as e:
      runner.get_log_position()
    assert (e.value.message ==
            "Unable to get the slave's log position: " +
            "Command 'exit 1' returned non-zero exit status 1")

  def test_stop_interminable(self):
    cmd = """trap "echo Trapped SIGTERM!" TERM
while :
do
  sleep 60
done
"""
    task_control = FakeTaskControl(mysqld=cmd)
    runner = MysosTaskRunner(
      self._self_instance,
      self._client,
      "/home/test/my_cluster",
      NoopPackageInstaller(),
      task_control,
      self._state_manager)

    task_control._mysqld = cmd
    runner.start()
    assert runner.stop(timeout=1)
    assert deadline(runner.join, Amount(1, Time.SECONDS)) == -signal.SIGKILL
