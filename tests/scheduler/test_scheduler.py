import getpass
import os
import shutil
import tempfile
import unittest

from mysos.common.testing import Fake
from mysos.scheduler.scheduler import (
    DEFAULT_TASK_CPUS,
    DEFAULT_TASK_DISK,
    DEFAULT_TASK_MEM,
    INCOMPATIBLE_ROLE_OFFER_REFUSE_DURATION,
    MysosScheduler)
from mysos.scheduler.launcher import create_resources
from mysos.scheduler.password import gen_encryption_key, PasswordBox
from mysos.scheduler.state import LocalStateProvider, MySQLCluster, Scheduler

from kazoo.handlers.threading import SequentialThreadingHandler
import mesos.interface.mesos_pb2 as mesos_pb2
from twitter.common import log
from twitter.common.metrics import RootMetrics
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


class TestScheduler(unittest.TestCase):
  def setUp(self):
    self._driver = FakeDriver()
    self._storage = FakeStorage(SequentialThreadingHandler())
    self._zk_client = FakeClient(storage=self._storage)
    self._zk_client.start()

    self._framework_id = mesos_pb2.FrameworkID()
    self._framework_id.value = "framework_id_0"

    self._offer = mesos_pb2.Offer()
    self._offer.id.value = "offer_id_0"
    self._offer.framework_id.value = self._framework_id.value
    self._offer.slave_id.value = "slave_id_0"
    self._offer.hostname = "localhost"

    resources = create_resources(
        cpus=DEFAULT_TASK_CPUS * 3,
        mem=DEFAULT_TASK_MEM * 3,
        disk=DEFAULT_TASK_DISK * 3,
        ports=set([10000, 10001, 10002]))
    self._offer.resources.extend(resources)

    self._framework_user = "framework_user"

    self._zk_url = "zk://host/mysos/test"
    self._cluster = MySQLCluster(
        "cluster0", "user", "pass", 3, DEFAULT_TASK_CPUS, DEFAULT_TASK_MEM, DEFAULT_TASK_DISK)

    self._tmpdir = tempfile.mkdtemp()
    self._state_provider = LocalStateProvider(self._tmpdir)

    framework_info = mesos_pb2.FrameworkInfo(
        user=getpass.getuser(),
        name="mysos",
        checkpoint=False)
    self._state = Scheduler(framework_info)

  def tearDown(self):
    shutil.rmtree(self._tmpdir, True)  # Clean up after ourselves.

  def test_scheduler_recovery(self):
    scheduler_key = gen_encryption_key()

    scheduler1 = MysosScheduler(
        self._state,
        self._state_provider,
        self._framework_user,
        "./executor.pex",
        "cmd.sh",
        self._zk_client,
        self._zk_url,
        Amount(5, Time.SECONDS),
        "/etc/mysos/admin_keyfile.yml",
        scheduler_key)
    scheduler1.registered(self._driver, self._framework_id, object())
    scheduler1.create_cluster("cluster1", "mysql_user", 3)
    scheduler1.resourceOffers(self._driver, [self._offer])

    # One task is launched for one offer.
    assert len(scheduler1._launchers["cluster1"]._cluster.tasks) == 1

    with pytest.raises(MysosScheduler.ClusterExists):
      scheduler1.create_cluster("cluster1", "mysql_user", 3)

    # FrameworkID should have been persisted.
    self._state = self._state_provider.load_scheduler_state()
    assert self._state.framework_info.id.value == self._framework_id.value

    # Simulate restart.
    scheduler2 = MysosScheduler(
        self._state,
        self._state_provider,
        self._framework_user,
        "./executor.pex",
        "cmd.sh",
        self._zk_client,
        self._zk_url,
        Amount(5, Time.SECONDS),
        "/etc/mysos/admin_keyfile.yml",
        scheduler_key)

    # Scheduler always receives registered() with the same FrameworkID after failover.
    scheduler2.registered(self._driver, self._framework_id, object())

    assert len(scheduler2._launchers) == 1
    assert scheduler2._launchers["cluster1"].cluster_name == "cluster1"

    # Scheduler has recovered the cluster so it doesn't accept another of the same name.
    with pytest.raises(MysosScheduler.ClusterExists):
      scheduler2.create_cluster("cluster1", "mysql_user", 3)

  def test_scheduler_recovery_failure_before_launch(self):
    scheduler_key = gen_encryption_key()

    scheduler1 = MysosScheduler(
        self._state,
        self._state_provider,
        self._framework_user,
        "./executor.pex",
        "cmd.sh",
        self._zk_client,
        self._zk_url,
        Amount(5, Time.SECONDS),
        "/etc/mysos/admin_keyfile.yml",
        scheduler_key)
    scheduler1.registered(self._driver, self._framework_id, object())
    _, password = scheduler1.create_cluster("cluster1", "mysql_user", 3)

    # Simulate restart before the task is successfully launched.
    scheduler2 = MysosScheduler(
        self._state,
        self._state_provider,
        self._framework_user,
        "./executor.pex",
        "cmd.sh",
        self._zk_client,
        self._zk_url,
        Amount(5, Time.SECONDS),
        "/etc/mysos/admin_keyfile.yml",
        scheduler_key)

    assert len(scheduler2._launchers) == 0  # No launchers are recovered.

    # Scheduler always receives registered() with the same FrameworkID after failover.
    scheduler2.registered(self._driver, self._framework_id, object())

    assert len(scheduler2._launchers) == 1
    assert scheduler2._launchers["cluster1"].cluster_name == "cluster1"

    password_box = PasswordBox(scheduler_key)

    assert password_box.match(
        password, scheduler2._launchers["cluster1"]._cluster.encrypted_password)

    # Now offer the resources for this task.
    scheduler2.resourceOffers(self._driver, [self._offer])

    # One task is launched for the offer.
    assert len(scheduler2._launchers["cluster1"]._cluster.active_tasks) == 1

    # Scheduler has recovered the cluster so it doesn't accept another of the same name.
    with pytest.raises(MysosScheduler.ClusterExists):
      scheduler2.create_cluster("cluster1", "mysql_user", 3)

  def test_incompatible_resource_role(self):
    scheduler1 = MysosScheduler(
        self._state,
        self._state_provider,
        self._framework_user,
        "./executor.pex",
        "cmd.sh",
        self._zk_client,
        self._zk_url,
        Amount(5, Time.SECONDS),
        "/etc/mysos/admin_keyfile.yml",
        gen_encryption_key(),
        framework_role='mysos')  # Require 'mysos' but the resources are in '*'.
    scheduler1.registered(self._driver, self._framework_id, object())
    scheduler1.create_cluster("cluster1", "mysql_user", 3)
    scheduler1.resourceOffers(self._driver, [self._offer])

    assert "declineOffer" in self._driver.method_calls
    assert len(self._driver.method_calls["declineOffer"]) == 1
    # [0][0][1]: [First declineOffer call][The positional args][The first positional arg], which is
    # a 'Filters' object.
    assert (self._driver.method_calls["declineOffer"][0][0][1].refuse_seconds ==
        INCOMPATIBLE_ROLE_OFFER_REFUSE_DURATION.as_(Time.SECONDS))

  def test_scheduler_metrics(self):
    scheduler_key = gen_encryption_key()

    scheduler = MysosScheduler(
        self._state,
        self._state_provider,
        self._framework_user,
        "./executor.pex",
        "cmd.sh",
        self._zk_client,
        self._zk_url,
        Amount(5, Time.SECONDS),
        "/etc/mysos/admin_keyfile.yml",
        scheduler_key)

    RootMetrics().register_observable('scheduler', scheduler)

    scheduler.registered(self._driver, self._framework_id, object())
    scheduler.create_cluster(
        "cluster1", "mysql_user", 3, cluster_password='test_password')

    sample = RootMetrics().sample()
    assert sample['scheduler.cluster_count'] == 1
    assert sample['scheduler.total_requested_mem_mb'] == DEFAULT_TASK_MEM.as_(Data.MB) * 3
    assert sample['scheduler.total_requested_disk_mb'] == DEFAULT_TASK_DISK.as_(Data.MB) * 3
    assert sample['scheduler.total_requested_cpus'] == DEFAULT_TASK_CPUS * 3

    scheduler.delete_cluster("cluster1", 'test_password')

    sample = RootMetrics().sample()
    assert sample['scheduler.cluster_count'] == 0
    assert sample['scheduler.total_requested_mem_mb'] == 0
    assert sample['scheduler.total_requested_disk_mb'] == 0
    assert sample['scheduler.total_requested_cpus'] == 0

  def test_scheduler_delete_empty_cluster(self):
    scheduler_key = gen_encryption_key()

    scheduler = MysosScheduler(
        self._state,
        self._state_provider,
        self._framework_user,
        "./executor.pex",
        "cmd.sh",
        self._zk_client,
        self._zk_url,
        Amount(5, Time.SECONDS),
        "/etc/mysos/admin_keyfile.yml",
        scheduler_key)

    scheduler.registered(self._driver, self._framework_id, object())
    _, password = scheduler.create_cluster("cluster1", "mysql_user", 3)

    assert len(scheduler._launchers) == 1

    # Deleting the cluster before any offer comes in for launching any task.
    scheduler.delete_cluster("cluster1", password)

    assert len(scheduler._launchers) == 0
