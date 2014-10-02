import Queue
import getpass
import os
import posixpath
import subprocess

from twitter.common import log
from twitter.common.zookeeper.serverset.endpoint import Endpoint, ServiceInstance
from twitter.mysos.common import zookeeper
from twitter.mysos.common.cluster import ClusterListener
from twitter.mysos.scheduler.scheduler import MysosScheduler

from kazoo.handlers.threading import SequentialThreadingHandler
import mesos.interface
from mesos.interface.mesos_pb2 import FrameworkInfo, DRIVER_STOPPED
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
  zk_root = zookeeper.parse(zk_url)[2]

  # Create a ClusterListener to monitor and verify the cluster state.
  # TODO(jyx): Update ClusterListener to allow a None 'self_instance' so we don't need to fake it.
  fake_instance = ServiceInstance(Endpoint("host", 51234))
  master = Queue.Queue()
  listener = ClusterListener(
      zk_client,
      posixpath.join(zk_root, "test_cluster"),
      fake_instance,
      master_callback=lambda x: master.put(x))
  listener.start()

  framework_info = FrameworkInfo(
      user=getpass.getuser(),
      name="mysos",
      checkpoint=False)

  scheduler = MysosScheduler(
      "fake_user",
      os.path.abspath("dist/testing_mysos_executor.pex"),
      "./testing_mysos_executor.pex",
      zk_client,
      zk_url)

  scheduler_driver = mesos.native.MesosSchedulerDriver(
      scheduler,
      framework_info,
      "local")

  scheduler_driver.start()

  scheduler.create_cluster("test_cluster", 3)

  # A random slave is promoted to be the master.
  assert master.get(True, 40)

  # Two slaves left.
  slaves = [path for path in storage.paths.keys() if path.startswith(
      "/home/mysos/clusters/test_cluster/slaves/member_")]
  assert len(slaves) == 2

  assert scheduler_driver.stop() == DRIVER_STOPPED
