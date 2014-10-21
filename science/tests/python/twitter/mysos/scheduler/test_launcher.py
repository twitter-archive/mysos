import os
import unittest

from twitter.common import log
from twitter.mysos.scheduler.launcher import create_resources, LauncherError, MySQLClusterLauncher
from twitter.mysos.common.testing import Fake

from kazoo.handlers.threading import SequentialThreadingHandler
import mesos.interface.mesos_pb2 as mesos_pb2
from zake.fake_client import FakeClient
from zake.fake_storage import FakeStorage

import pytest


if 'MYSOS_DEBUG' in os.environ:
  from twitter.common.log.options import LogOptions
  LogOptions.set_stderr_log_level('google:DEBUG')
  LogOptions.set_simple(True)
  log.init('mysos_tests')


class FakeDriver(Fake): pass


class TestLauncher(unittest.TestCase):
  def setUp(self):
    self._driver = FakeDriver()
    self._storage = FakeStorage(SequentialThreadingHandler())
    self._zk_client = FakeClient(storage=self._storage)
    self._zk_client.start()

    self._offer = mesos_pb2.Offer()
    self._offer.id.value = "offer_id_0"
    self._offer.framework_id.value = "framework_id_0"
    self._offer.slave_id.value = "slave_id_0"
    self._offer.hostname = "localhost"

  def test_launch_cluster_all_nodes_successful(self):
    num_nodes = 3
    launcher = MySQLClusterLauncher(
        "zk://host/mysos/test",
        self._zk_client,
        "cluster0",
        num_nodes,
        "cmd.sh",
        "./executor.pex")

    resources = create_resources(cpus=4, mem=1024, ports=set([10000, 10001, 10002]))
    self._offer.resources.extend(resources)

    for i in range(num_nodes):
      task_id, remaining = launcher.launch(self._driver, self._offer)
      del self._offer.resources[:]
      self._offer.resources.extend(remaining)
      assert task_id == "mysos-cluster0-%s" % i

    tasks = self._driver.method_calls["launchTasks"]
    assert len(tasks) == num_nodes

    # No new tasks are launched.
    assert launcher.launch(self._driver, self._offer)[0] is None
    assert len(self._driver.method_calls["launchTasks"]) == num_nodes

    # All 3 nodes have successfully started.
    status = mesos_pb2.TaskStatus()
    status.state = mesos_pb2.TASK_RUNNING  # Valid state.
    for i in range(num_nodes):
      status.task_id.value = "mysos-cluster0-%s" % i
      launcher.status_update(status)

    # One master is elected.
    master = "/mysos/test/cluster0/master/member_"
    assert len([x for x in self._storage.paths.keys() if x.startswith(master)]) == 1

  def test_launch_cluster_insufficient_resources(self):
    num_nodes = 3
    launcher = MySQLClusterLauncher(
        "zk://host/mysos/test",
        self._zk_client,
        "cluster0",
        num_nodes,
        "cmd.sh",
        "./executor.pex")

    resources = create_resources(cpus=4, mem=1024, ports=set([10000, 100001]))
    self._offer.resources.extend(resources)

    # There is one fewer port than required to launch the entire cluster.
    for i in range(num_nodes - 1):
      task_id, remaining = launcher.launch(self._driver, self._offer)
      del self._offer.resources[:]
      self._offer.resources.extend(remaining)
      assert task_id == "mysos-cluster0-%s" % i

    tasks = self._driver.method_calls["launchTasks"]
    assert len(tasks) == num_nodes - 1

    # The final task cannot get launched.
    assert launcher.launch(self._driver, self._offer)[0] is None
    assert len(self._driver.method_calls["launchTasks"]) == num_nodes - 1

  def test_two_launchers(self):
    launchers = [
      MySQLClusterLauncher(
          "zk://host/mysos/test",
          self._zk_client,
          "cluster0",
          1,
          "cmd.sh",
          "./executor.pex"),
      MySQLClusterLauncher(
          "zk://host/mysos/test",
          self._zk_client,
          "cluster1",
          2,
          "cmd.sh",
          "./executor.pex")]

    resources = create_resources(cpus=4, mem=1024, ports=set([10000, 10001, 10002]))
    self._offer.resources.extend(resources)

    # Three nodes in total across two clusters.
    # Simulate the scheduler.
    for i in range(3):
      for launcher in launchers:
        task_id, remaining = launcher.launch(self._driver, self._offer)
        if task_id:
          # Update the offer so other launchers will use its remaining resources.
          del self._offer.resources[:]
          self._offer.resources.extend(remaining)
          break

    tasks = self._driver.method_calls["launchTasks"]
    assert len(tasks) == 3

  def test_invalid_status_update(self):
    num_nodes = 1
    launcher = MySQLClusterLauncher(
        "zk://host/mysos/test",
        self._zk_client,
        "cluster0",
        num_nodes,
        "cmd.sh",
        "./executor.pex")

    resources = create_resources(cpus=4, mem=1024, ports=set([10000]))
    self._offer.resources.extend(resources)

    task_id, _ = launcher.launch(self._driver, self._offer)
    assert task_id == "mysos-cluster0-0"

    tasks = self._driver.method_calls["launchTasks"]
    assert len(tasks) == num_nodes

    status = mesos_pb2.TaskStatus()
    status.task_id.value = task_id
    status.state = mesos_pb2.TASK_RUNNING  # Valid state.
    launcher.status_update(status)

    status.state = mesos_pb2.TASK_FINISHED  # An invalid state.

    with pytest.raises(LauncherError):
      launcher.status_update(status)

  def test_terminal_status_update(self):
    num_nodes = 1
    launcher = MySQLClusterLauncher(
        "zk://host/mysos/test",
        self._zk_client,
        "cluster0",
        num_nodes,
        "cmd.sh",
        "./executor.pex")

    resources = create_resources(cpus=4, mem=1024, ports=set([10000]))
    self._offer.resources.extend(resources)

    task_id, _ = launcher.launch(self._driver, self._offer)
    assert task_id == "mysos-cluster0-0"

    tasks = self._driver.method_calls["launchTasks"]
    assert len(tasks) == num_nodes

    status = mesos_pb2.TaskStatus()
    status.task_id.value = task_id
    status.state = mesos_pb2.TASK_RUNNING
    launcher.status_update(status)

    assert launcher._cluster.running_tasks == 1

    status.state = mesos_pb2.TASK_LOST
    launcher.status_update(status)

    assert launcher._cluster.running_tasks == 0
