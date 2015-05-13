import json
import os
import shutil
import tempfile
import threading

from mysos.common.cluster import get_cluster_path, wait_for_master
from mysos.common.testing import Fake
from mysos.scheduler.launcher import create_resources, MySQLClusterLauncher
from mysos.scheduler.password import gen_encryption_key, PasswordBox
from mysos.scheduler.scheduler import DEFAULT_TASK_CPUS, DEFAULT_TASK_MEM, DEFAULT_TASK_DISK
from mysos.scheduler.state import LocalStateProvider, MySQLCluster
from mysos.scheduler.zk_state import ZooKeeperStateProvider

from kazoo.handlers.threading import SequentialThreadingHandler
import mesos.interface.mesos_pb2 as mesos_pb2
from twitter.common import log
from twitter.common.concurrent import deadline
from twitter.common.quantity import Amount, Data, Time
from zake.fake_client import FakeClient
from zake.fake_storage import FakeStorage

import pytest


if 'MYSOS_DEBUG' in os.environ:
  from twitter.common.log.options import LogOptions
  LogOptions.set_stderr_log_level('google:DEBUG')
  LogOptions.set_simple(True)
  log.init('mysos_tests')


class FakeDriver(Fake): pass


class TestLauncher(object):
  @pytest.fixture(params=[LocalStateProvider, ZooKeeperStateProvider], autouse=True)
  def setup(self, request):
    self._driver = FakeDriver()
    self._storage = FakeStorage(SequentialThreadingHandler())
    self._zk_client = FakeClient(storage=self._storage)
    self._zk_client.start()

    self._offer = mesos_pb2.Offer()
    self._offer.id.value = "offer_id_0"
    self._offer.framework_id.value = "framework_id_0"
    self._offer.slave_id.value = "slave_id_0"
    self._offer.hostname = "localhost"

    # Enough resources to fit three tasks.
    resources = create_resources(
        cpus=DEFAULT_TASK_CPUS * 3,
        mem=DEFAULT_TASK_MEM * 3,
        disk=DEFAULT_TASK_DISK * 3,
        ports=set([10000, 10001, 10002]))
    self._offer.resources.extend(resources)

    self._framework_user = "framework_user"

    # Some tests use the default launcher; some don't.
    self._zk_url = "zk://host/mysos/test"

    self._scheduler_key = gen_encryption_key()
    self._password_box = PasswordBox(self._scheduler_key)

    self._cluster = MySQLCluster(
        "cluster0",
        "user",
        self._password_box.encrypt("pass"),
        3,
        DEFAULT_TASK_CPUS,
        DEFAULT_TASK_MEM,
        DEFAULT_TASK_DISK)

    # Construct the state provider based on the test parameter.
    if request.param == LocalStateProvider:
      tmpdir = tempfile.mkdtemp()
      self._state_provider = LocalStateProvider(tmpdir)
      request.addfinalizer(lambda: shutil.rmtree(tmpdir, True))  # Clean up after ourselves.
    elif request.param == ZooKeeperStateProvider:
      self._state_provider = ZooKeeperStateProvider(self._zk_client, "/mysos/test")

    self._launcher = MySQLClusterLauncher(
        self._driver,
        self._cluster,
        self._state_provider,
        self._zk_url,
        self._zk_client,
        self._framework_user,
        "./executor.pex",
        "cmd.sh",
        Amount(5, Time.SECONDS),
        "/etc/mysos/admin_keyfile.yml",
        self._scheduler_key,
        query_interval=Amount(150, Time.MILLISECONDS))  # Short interval.

    self._elected = threading.Event()
    self._launchers = [self._launcher]  # See teardown().

    request.addfinalizer(self.teardown)

  def teardown(self):
    for launcher in self._launchers:
      if launcher._elector:
        launcher._elector.abort()  # Abort the thread even if the election is pending.
        launcher._elector.join()

  def test_launch_cluster_all_nodes_successful(self):
    for i in range(self._cluster.num_nodes):
      task_id, remaining = self._launcher.launch(self._offer)
      del self._offer.resources[:]
      self._offer.resources.extend(remaining)
      assert task_id == "mysos-cluster0-%s" % i

    tasks = self._driver.method_calls["launchTasks"]
    assert len(tasks) == self._cluster.num_nodes

    # No new tasks are launched.
    assert self._launcher.launch(self._offer)[0] is None
    assert len(self._driver.method_calls["launchTasks"]) == self._cluster.num_nodes

    # All 3 nodes have successfully started.
    status = mesos_pb2.TaskStatus()
    status.state = mesos_pb2.TASK_RUNNING  # Valid state.
    status.slave_id.value = self._offer.slave_id.value
    for i in range(self._cluster.num_nodes):
      status.task_id.value = "mysos-cluster0-%s" % i
      self._launcher.status_update(status)

    deadline(
        lambda: wait_for_master(
            get_cluster_path(self._zk_url, self._cluster.name),
            self._zk_client),
        Amount(5, Time.SECONDS))

    # The first slave is elected.
    assert "/mysos/test/cluster0/master/member_0000000000" in self._storage.paths
    # Two slaves.
    assert len([x for x in self._storage.paths.keys() if x.startswith(
        "/mysos/test/cluster0/slaves/member_")]) == 2

  def test_launch_cluster_insufficient_resources(self):
    """All but one slave in the slave are launched successfully."""
    del self._offer.resources[:]
    resources = create_resources(
        cpus=DEFAULT_TASK_CPUS * 3,
        mem=DEFAULT_TASK_MEM * 3,
        disk=DEFAULT_TASK_DISK * 3 - Amount(1, Data.MB),  # 1mb less than required disk space.
        ports=set([10000, 10001, 10002]))
    self._offer.resources.extend(resources)

    # There is one fewer port than required to launch the entire cluster.
    for i in range(self._cluster.num_nodes - 1):
      task_id, remaining = self._launcher.launch(self._offer)
      del self._offer.resources[:]
      self._offer.resources.extend(remaining)
      assert task_id == "mysos-cluster0-%s" % i

    tasks = self._driver.method_calls["launchTasks"]
    assert len(tasks) == self._cluster.num_nodes - 1

    # The final task cannot get launched.
    assert self._launcher.launch(self._offer)[0] is None
    assert len(self._driver.method_calls["launchTasks"]) == self._cluster.num_nodes - 1

    # The two nodes have successfully started.
    status = mesos_pb2.TaskStatus()
    status.state = mesos_pb2.TASK_RUNNING  # Valid state.
    status.slave_id.value = self._offer.slave_id.value
    for i in range(self._cluster.num_nodes - 1):
      status.task_id.value = "mysos-cluster0-%s" % i
      self._launcher.status_update(status)

    deadline(
        lambda: wait_for_master(
            get_cluster_path(self._zk_url, self._cluster.name),
            self._zk_client),
        Amount(5, Time.SECONDS))

    # The first slave is elected.
    assert "/mysos/test/cluster0/master/member_0000000000" in self._storage.paths
    # One slave.
    assert len([x for x in self._storage.paths.keys() if x.startswith(
      "/mysos/test/cluster0/slaves/member_")]) == 1

  def test_two_launchers(self):
    """Two launchers share resources and launch their clusters successfully."""
    launchers = [
      MySQLClusterLauncher(
          self._driver,
          MySQLCluster(
              "cluster0",
              "user0",
              self._password_box.encrypt("pass0"),
              1,
              DEFAULT_TASK_CPUS,
              DEFAULT_TASK_MEM,
              DEFAULT_TASK_DISK),
          self._state_provider,
          self._zk_url,
          self._zk_client,
          self._framework_user,
          "./executor.pex",
          "cmd.sh",
          Amount(5, Time.SECONDS),
          "/etc/mysos/admin_keyfile.yml",
          self._scheduler_key),
      MySQLClusterLauncher(
          self._driver,
          MySQLCluster(
              "cluster1",
              "user1",
              self._password_box.encrypt("pass1"),
              2,
              DEFAULT_TASK_CPUS,
              DEFAULT_TASK_MEM,
              DEFAULT_TASK_DISK),
          self._state_provider,
          self._zk_url,
          self._zk_client,
          self._framework_user,
          "./executor.pex",
          "cmd.sh",
          Amount(5, Time.SECONDS),
          "/etc/mysos/admin_keyfile.yml",
          self._scheduler_key)]
    self._launchers.extend(launchers)

    resources = create_resources(
        cpus=DEFAULT_TASK_CPUS * 3,
        mem=DEFAULT_TASK_MEM * 3,
        disk=DEFAULT_TASK_DISK * 3,
        ports=set([10000, 10001, 10002]))
    self._offer.resources.extend(resources)

    # Three nodes in total across two clusters.
    # Simulate the scheduler.
    for i in range(3):
      for launcher in launchers:
        task_id, remaining = launcher.launch(self._offer)
        if task_id:
          # Update the offer so other launchers will use its remaining resources.
          del self._offer.resources[:]
          self._offer.resources.extend(remaining)
          break

    tasks = self._driver.method_calls["launchTasks"]
    assert len(tasks) == 3

  def test_invalid_status_update(self):
    """Launcher raises an exception when an invalid status is received."""
    self._cluster.num_nodes = 1
    launcher = MySQLClusterLauncher(
        self._driver,
        self._cluster,
        self._state_provider,
        self._zk_url,
        self._zk_client,
        self._framework_user,
        "./executor.pex",
        "cmd.sh",
        Amount(5, Time.SECONDS),
        "/etc/mysos/admin_keyfile.yml",
        self._scheduler_key)
    self._launchers.append(launcher)

    resources = create_resources(
        cpus=DEFAULT_TASK_CPUS,
        mem=DEFAULT_TASK_MEM,
        disk=DEFAULT_TASK_DISK,
        ports=set([10000]))
    self._offer.resources.extend(resources)

    task_id, _ = launcher.launch(self._offer)
    assert task_id == "mysos-cluster0-0"

    tasks = self._driver.method_calls["launchTasks"]
    assert len(tasks) == self._cluster.num_nodes

    status = mesos_pb2.TaskStatus()
    status.task_id.value = task_id
    status.state = mesos_pb2.TASK_RUNNING  # Valid state.
    launcher.status_update(status)

    status.state = mesos_pb2.TASK_FINISHED  # An invalid state.

    with pytest.raises(MySQLClusterLauncher.Error):
      launcher.status_update(status)

  def test_terminal_status_update(self):
    """Launcher reacts to terminated task by launching a new one."""
    self._cluster.num_nodes = 1
    launcher = MySQLClusterLauncher(
        self._driver,
        self._cluster,
        self._state_provider,
        self._zk_url,
        self._zk_client,
        self._framework_user,
        "./executor.pex",
        "cmd.sh",
        Amount(1, Time.SECONDS),
        "/etc/mysos/admin_keyfile.yml",
        self._scheduler_key)
    self._launchers.append(launcher)

    resources = create_resources(
        cpus=DEFAULT_TASK_CPUS,
        mem=DEFAULT_TASK_MEM,
        disk=DEFAULT_TASK_DISK,
        ports=set([10000]))
    self._offer.resources.extend(resources)

    task_id, _ = launcher.launch(self._offer)
    assert task_id == "mysos-cluster0-0"

    launched = self._driver.method_calls["launchTasks"]
    assert len(launched) == self._cluster.num_nodes

    status = mesos_pb2.TaskStatus()
    status.task_id.value = task_id
    status.state = mesos_pb2.TASK_RUNNING
    launcher.status_update(status)

    assert len(launcher._cluster.running_tasks) == 1

    status.state = mesos_pb2.TASK_LOST
    launcher.status_update(status)

    assert len(launcher._cluster.running_tasks) == 0

    task_id, _ = launcher.launch(self._offer)
    assert task_id == "mysos-cluster0-1"

    launched = self._driver.method_calls["launchTasks"]

    # One task is relaunched to make up for the lost one.
    assert len(launched) == self._cluster.num_nodes + 1

  def test_master_failover(self):
    for i in range(self._cluster.num_nodes):
      task_id, remaining = self._launcher.launch(self._offer)
      del self._offer.resources[:]
      self._offer.resources.extend(remaining)
      assert task_id == "mysos-cluster0-%s" % i

    tasks = self._driver.method_calls["launchTasks"]
    assert len(tasks) == self._cluster.num_nodes

    # All 3 nodes have successfully started.
    status = mesos_pb2.TaskStatus()
    status.state = mesos_pb2.TASK_RUNNING
    status.slave_id.value = self._offer.slave_id.value

    for i in range(self._cluster.num_nodes):
      status.task_id.value = "mysos-cluster0-%s" % i
      self._launcher.status_update(status)

    # No log positions queries are sent for the first epoch.
    assert "sendFrameworkMessage" not in self._driver.method_calls

    # Wait for the election to complete.
    deadline(
        lambda: wait_for_master(
            get_cluster_path(self._zk_url, self._cluster.name),
            self._zk_client),
        Amount(5, Time.SECONDS))

    # The first slave is elected.
    assert "/mysos/test/cluster0/master/member_0000000000" in self._storage.paths

    # Now fail the master task.
    status.task_id.value = "mysos-cluster0-0"
    status.state = mesos_pb2.TASK_FAILED
    self._launcher.status_update(status)

    assert len(self._launcher._cluster.running_tasks) == 2

    # Log positions queries are sent.
    self._launcher._elector._elect()
    assert len(self._driver.method_calls["sendFrameworkMessage"]) >= 2

    for i in range(1, self._cluster.num_nodes):
      self._launcher.framework_message(
          "mysos-cluster0-%s" % i,
          self._offer.slave_id.value,
          json.dumps(dict(epoch=1, position=str(i))))

    # Wait for the election to complete.
    deadline(
        lambda: wait_for_master(
            get_cluster_path(self._zk_url, self._cluster.name),
            self._zk_client),
        Amount(5, Time.SECONDS))

    # The slave with the highest position is elected.
    assert "/mysos/test/cluster0/master/member_0000000002" in self._storage.paths

    assert len(self._launcher._cluster.running_tasks) == 2

    # When a new offer comes in, a new task is launched.
    del self._offer.resources[:]
    resources = create_resources(
        cpus=DEFAULT_TASK_CPUS,
        mem=DEFAULT_TASK_MEM,
        disk=DEFAULT_TASK_DISK,
        ports=set([10000]))
    self._offer.resources.extend(resources)
    task_id, _ = self._launcher.launch(self._offer)
    assert task_id == "mysos-cluster0-3"

    launched = self._driver.method_calls["launchTasks"]
    # One task is relaunched to make up for the failed one.
    assert len(launched) == self._cluster.num_nodes + 1

  def test_launcher_recovery_after_election_completed(self):
    # 1. Launch a cluster on the running launcher.
    for i in range(self._cluster.num_nodes):
      task_id, remaining = self._launcher.launch(self._offer)
      del self._offer.resources[:]
      self._offer.resources.extend(remaining)
      assert task_id == "mysos-cluster0-%s" % i

    tasks = self._driver.method_calls["launchTasks"]
    assert len(tasks) == self._cluster.num_nodes

    # No new tasks are launched.
    assert self._launcher.launch(self._offer)[0] is None
    assert len(self._driver.method_calls["launchTasks"]) == self._cluster.num_nodes

    # All 3 nodes have successfully started.
    status = mesos_pb2.TaskStatus()
    status.state = mesos_pb2.TASK_RUNNING
    status.slave_id.value = self._offer.slave_id.value
    for i in range(self._cluster.num_nodes):
      status.task_id.value = "mysos-cluster0-%s" % i
      self._launcher.status_update(status)

    deadline(
        lambda: wait_for_master(
            get_cluster_path(self._zk_url, self._cluster.name),
            self._zk_client),
        Amount(5, Time.SECONDS))

    # The first slave is elected.
    assert "/mysos/test/cluster0/master/member_0000000000" in self._storage.paths
    # Two slaves.
    assert len([x for x in self._storage.paths.keys() if x.startswith(
        "/mysos/test/cluster0/slaves/member_")]) == 2

    # 2. Recover the launcher.
    self._cluster = self._state_provider.load_cluster_state(self._cluster.name)
    self._launcher = MySQLClusterLauncher(
        self._driver,
        self._cluster,
        self._state_provider,
        self._zk_url,
        self._zk_client,
        self._framework_user,
        "./executor.pex",
        "cmd.sh",
        Amount(5, Time.SECONDS),
        "/etc/mysos/admin_keyfile.yml",
        self._scheduler_key,
        query_interval=Amount(150, Time.MILLISECONDS))

    # Now fail the master task.
    status.task_id.value = "mysos-cluster0-0"
    status.state = mesos_pb2.TASK_FAILED
    self._launcher.status_update(status)

    for i in range(1, self._cluster.num_nodes):
      self._launcher.framework_message(
          "mysos-cluster0-%s" % i,
          self._offer.slave_id.value,
          json.dumps(dict(epoch=1, position=str(i))))

    deadline(
        lambda: wait_for_master(
            get_cluster_path(self._zk_url, self._cluster.name),
            self._zk_client),
        Amount(5, Time.SECONDS))

    # The second slave has the larger position and is elected.
    assert "/mysos/test/cluster0/master/member_0000000002" in self._storage.paths

  def test_launcher_recovery_before_election_completed(self):
    # 1. Launch a cluster on the running launcher.
    for i in range(self._cluster.num_nodes):
      task_id, remaining = self._launcher.launch(self._offer)
      del self._offer.resources[:]
      self._offer.resources.extend(remaining)
      assert task_id == "mysos-cluster0-%s" % i

    tasks = self._driver.method_calls["launchTasks"]
    assert len(tasks) == self._cluster.num_nodes

    # No new tasks are launched.
    assert self._launcher.launch(self._offer)[0] is None
    assert len(self._driver.method_calls["launchTasks"]) == self._cluster.num_nodes

    # All 3 nodes have successfully started.
    status = mesos_pb2.TaskStatus()
    status.state = mesos_pb2.TASK_RUNNING
    status.slave_id.value = self._offer.slave_id.value
    for i in range(self._cluster.num_nodes):
      status.task_id.value = "mysos-cluster0-%s" % i
      self._launcher.status_update(status)

    deadline(
        lambda: wait_for_master(
            get_cluster_path(self._zk_url, self._cluster.name),
            self._zk_client),
        Amount(5, Time.SECONDS))

    # The first slave is elected.
    assert "/mysos/test/cluster0/master/member_0000000000" in self._storage.paths
    # Two slaves.
    assert len([x for x in self._storage.paths.keys() if x.startswith(
        "/mysos/test/cluster0/slaves/member_")]) == 2

    # Now fail the master task which leads to re-election.
    status.task_id.value = "mysos-cluster0-0"
    status.state = mesos_pb2.TASK_FAILED
    self._launcher.status_update(status)

    # 2. Recover the launcher.
    self._cluster = self._state_provider.load_cluster_state(self._cluster.name)
    self._launcher = MySQLClusterLauncher(
        self._driver,
        self._cluster,
        self._state_provider,
        self._zk_url,
        self._zk_client,
        self._framework_user,
        "./executor.pex",
        "cmd.sh",
        Amount(5, Time.SECONDS),
        "/etc/mysos/admin_keyfile.yml",
        self._scheduler_key,
        query_interval=Amount(150, Time.MILLISECONDS))

    for i in range(1, self._cluster.num_nodes):
      self._launcher.framework_message(
          "mysos-cluster0-%s" % i,
          self._offer.slave_id.value,
          json.dumps(dict(epoch=2, position=str(i))))

    deadline(
        lambda: wait_for_master(
            get_cluster_path(self._zk_url, self._cluster.name),
            self._zk_client),
        Amount(5, Time.SECONDS))

    # The second slave has the larger position and is elected.
    assert "/mysos/test/cluster0/master/member_0000000002" in self._storage.paths

  def test_launcher_kill(self):
    for i in range(self._cluster.num_nodes):
      task_id, remaining = self._launcher.launch(self._offer)
      del self._offer.resources[:]
      self._offer.resources.extend(remaining)
      assert task_id == "mysos-cluster0-%s" % i

    tasks = self._driver.method_calls["launchTasks"]
    assert len(tasks) == self._cluster.num_nodes

    # No new tasks are launched.
    assert self._launcher.launch(self._offer)[0] is None
    assert len(self._driver.method_calls["launchTasks"]) == self._cluster.num_nodes

    # All 3 nodes have successfully started.
    status = mesos_pb2.TaskStatus()
    status.state = mesos_pb2.TASK_RUNNING  # Valid state.
    status.slave_id.value = self._offer.slave_id.value
    for i in range(self._cluster.num_nodes):
      status.task_id.value = "mysos-cluster0-%s" % i
      self._launcher.status_update(status)

    deadline(
        lambda: wait_for_master(
            get_cluster_path(self._zk_url, self._cluster.name),
            self._zk_client),
        Amount(5, Time.SECONDS))

    # The first slave is elected.
    assert "/mysos/test/cluster0/master/member_0000000000" in self._storage.paths
    # Two slaves.
    assert len([x for x in self._storage.paths.keys() if x.startswith(
        "/mysos/test/cluster0/slaves/member_")]) == 2

    # Kill the cluster.
    with pytest.raises(MySQLClusterLauncher.PermissionError):
      self._launcher.kill("wrong_password")

    # Correct password.
    self._launcher.kill(self._password_box.decrypt(self._cluster.encrypted_password))

    # All 3 nodes are successfully killed.
    status = mesos_pb2.TaskStatus()
    status.state = mesos_pb2.TASK_KILLED
    status.slave_id.value = self._offer.slave_id.value
    for i in range(self._cluster.num_nodes):
      status.task_id.value = "mysos-cluster0-%s" % i
      self._launcher.status_update(status)

    assert "/mysos/test/cluster0" not in self._storage.paths  # ServerSets removed.
    assert not self._state_provider.load_cluster_state("cluster0")  # State removed.

  def test_launcher_recovery_corrupted_password(self):
    # 1. Launch a single instance for a cluster on the running launcher.
    task_id, remaining = self._launcher.launch(self._offer)
    del self._offer.resources[:]
    self._offer.resources.extend(remaining)
    assert task_id == "mysos-cluster0-0"

    # The task has successfully started.
    status = mesos_pb2.TaskStatus()
    status.state = mesos_pb2.TASK_RUNNING
    status.slave_id.value = self._offer.slave_id.value
    status.task_id.value = "mysos-cluster0-0"
    self._launcher.status_update(status)

    # 2. Recover the launcher.
    self._cluster = self._state_provider.load_cluster_state(self._cluster.name)
    self._cluster.encrypted_password = "corrupted_password"

    # The corrupted password causes the launcher constructor to fail.
    with pytest.raises(ValueError):
      self._launcher = MySQLClusterLauncher(
          self._driver,
          self._cluster,
          self._state_provider,
          self._zk_url,
          self._zk_client,
          self._framework_user,
          "./executor.pex",
          "cmd.sh",
          Amount(5, Time.SECONDS),
          "/etc/mysos/admin_keyfile.yml",
          self._scheduler_key,
          query_interval=Amount(150, Time.MILLISECONDS))
