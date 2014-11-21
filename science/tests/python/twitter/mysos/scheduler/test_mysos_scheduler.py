import getpass
import os
import subprocess

from twitter.common import log
from twitter.common.concurrent import deadline
from twitter.common.quantity import Amount, Time
from twitter.mysos.common.cluster import get_cluster_path, wait_for_master
from twitter.mysos.scheduler.scheduler import MysosScheduler

from kazoo.handlers.threading import SequentialThreadingHandler
import mesos.interface
from mesos.interface.mesos_pb2 import DRIVER_STOPPED, FrameworkInfo
import mesos.native
from zake.fake_client import FakeClient
from zake.fake_storage import FakeStorage


if 'MYSOS_DEBUG' in os.environ:
  from twitter.common.log.options import LogOptions
  LogOptions.set_stderr_log_level('google:DEBUG')
  LogOptions.set_simple(True)
  log.init('mysos_tests')


def test_scheduler_runs():
  """
    Verifies that the scheduler successfully launches 3 "no-op" MySQL tasks.
    NOTE: Due to the limitation of zake the scheduler's ZK operations are not propagated to
    executors in separate processes but they are unit-tested separately.
  """

  # Make sure testing_mysos_executor.pex is built and available to be fetched by Mesos slave.
  assert subprocess.call(
      ["./pants", "tests/python/twitter/mysos/executor:testing_mysos_executor"]) == 0

  storage = FakeStorage(SequentialThreadingHandler())
  zk_client = FakeClient(storage=storage)
  zk_client.start()

  zk_url = "zk://fake_host/home/mysos/clusters"
  cluster_name = "test_cluster"
  num_nodes = 3

  framework_info = FrameworkInfo(
      user=getpass.getuser(),
      name="mysos",
      checkpoint=False)

  scheduler = MysosScheduler(
      "fake_user",
      os.path.abspath("dist/testing_mysos_executor.pex"),
      "./testing_mysos_executor.pex",
      zk_client,
      zk_url,
      election_timeout=Amount(40, Time.SECONDS))

  scheduler_driver = mesos.native.MesosSchedulerDriver(
      scheduler,
      framework_info,
      "local")
  scheduler_driver.start()

  scheduler.create_cluster(cluster_name, num_nodes)

  # A slave is promoted to be the master.
  deadline(
      lambda: wait_for_master(
          get_cluster_path(zk_url, cluster_name),
          zk_client),
      Amount(40, Time.SECONDS))

  assert scheduler_driver.stop() == DRIVER_STOPPED
