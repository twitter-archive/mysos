import json
import os
import posixpath
import socket

from twitter.common import app, log
from twitter.common.log.options import LogOptions
from twitter.common.zookeeper.serverset.endpoint import Endpoint, ServiceInstance
from twitter.mysos.common import zookeeper
from twitter.mysos.common.testing import Fake
from twitter.mysos.executor.executor import MysosExecutor
from twitter.mysos.executor.mysos_task_runner import MysosTaskRunner, TaskRunnerProvider
from twitter.mysos.executor.noop_installer import NoopPackageInstaller
from twitter.mysos.executor.sandbox import Sandbox

from .fake import FakeTaskControlProvider

import mesos.native
from zake.fake_client import FakeClient


SANDBOX_ROOT = os.path.join(os.path.realpath('.'), "sandbox")


class FakeTaskRunnerProvider(TaskRunnerProvider):
  """
    Creates a MysosTaskRunner with FakeTaskControl for testing purposes.
    NOTE: zake is used so a running ZK server is not required.
  """

  def __init__(self, task_control_provider):
    self._task_control_provider = task_control_provider

  def from_task(self, task, sandbox):
    data = json.loads(task.data)
    cluster_name, port, zk_url = data['cluster'], data['port'], data['zk_url']

    _, servers, path = zookeeper.parse(zk_url)

    zk_client = FakeClient()
    zk_client.start()
    self_instance = ServiceInstance(Endpoint(socket.gethostbyname(socket.gethostname()), port))
    task_control = self._task_control_provider.from_task(task, sandbox)

    return MysosTaskRunner(
        self_instance,
        zk_client,
        posixpath.join(path, cluster_name),
        NoopPackageInstaller(),
        task_control,
        Fake())


def main(args, options):
  log.info('Starting testing mysos executor')

  executor = MysosExecutor(
      FakeTaskRunnerProvider(FakeTaskControlProvider()), Sandbox(SANDBOX_ROOT))

  driver = mesos.native.MesosExecutorDriver(executor)
  driver.run()

  log.info('Exiting executor main')

LogOptions.disable_disk_logging()

# This is a testing executor. We log more verbosely.
LogOptions.set_stderr_log_level('google:DEBUG')
app.main()
