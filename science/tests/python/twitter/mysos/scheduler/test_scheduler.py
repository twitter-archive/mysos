import getpass
import os
import shutil
import tempfile
import unittest

from twitter.common import log
from twitter.common.quantity import Amount, Time
from twitter.mysos.common.testing import Fake
from twitter.mysos.scheduler.scheduler import MysosScheduler
from twitter.mysos.scheduler.launcher import create_resources
from twitter.mysos.scheduler.state import LocalStateProvider, MySQLCluster, Scheduler

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

    resources = create_resources(cpus=4, mem=512 * 3, ports=set([10000, 10001, 10002]))
    self._offer.resources.extend(resources)

    self._framework_user = "framework_user"

    self._zk_url = "zk://host/mysos/test"
    self._cluster = MySQLCluster("cluster0", "user", "pass", 3)

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
        "hdfs://host/path")
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
        "hdfs://host/path")
    assert len(scheduler2._launchers) == 1
    assert scheduler2._launchers["cluster1"].cluster_name == "cluster1"

    scheduler2.reregistered(self._driver, object())

    # Scheduler has recovered the cluster so it doesn't accept another of the same name.
    with pytest.raises(MysosScheduler.ClusterExists):
      scheduler2.create_cluster("cluster1", "mysql_user", 3)
