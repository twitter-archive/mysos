import getpass
import os
import posixpath

from mysos.common.cluster import get_cluster_path, wait_for_master
from mysos.scheduler.scheduler import MysosScheduler
from mysos.scheduler.state import LocalStateProvider, Scheduler

from kazoo.handlers.threading import SequentialThreadingHandler
from mesos.interface.mesos_pb2 import DRIVER_STOPPED, FrameworkInfo
from twitter.common import log
from twitter.common.concurrent import deadline
from twitter.common.dirutil import safe_mkdtemp
from twitter.common.quantity import Amount, Time
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
  import mesos.native

  # Make sure fake_mysos_executor.pex is available to be fetched by Mesos slave.
  assert os.path.isfile('dist/fake_mysos_executor.pex')

  storage = FakeStorage(SequentialThreadingHandler())
  zk_client = FakeClient(storage=storage)
  zk_client.start()

  zk_url = "zk://fake_host/home/mysos/clusters"
  cluster_name = "test_cluster"
  num_nodes = 3

  state_provider = LocalStateProvider(safe_mkdtemp())

  framework_info = FrameworkInfo(
      user=getpass.getuser(),
      name="mysos",
      checkpoint=False)

  state = Scheduler(framework_info)

  scheduler = MysosScheduler(
      state,
      state_provider,
      getpass.getuser(),
      os.path.abspath("dist/fake_mysos_executor.pex"),
      "./fake_mysos_executor.pex",
      zk_client,
      zk_url,
      Amount(40, Time.SECONDS),
      "/fakepath")

  scheduler_driver = mesos.native.MesosSchedulerDriver(
      scheduler,
      framework_info,
      "local")
  scheduler_driver.start()

  # Wait until the scheduler is connected and becomes available.
  assert scheduler.connected.wait(30)

  scheduler.create_cluster(cluster_name, "mysql_user", num_nodes)

  # A slave is promoted to be the master.
  deadline(
      lambda: wait_for_master(
          get_cluster_path(posixpath.join(zk_url, 'discover'), cluster_name),
          zk_client),
      Amount(40, Time.SECONDS))

  assert scheduler_driver.stop() == DRIVER_STOPPED
