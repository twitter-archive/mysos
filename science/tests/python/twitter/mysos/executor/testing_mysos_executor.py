import json
import posixpath
import socket

from twitter.common import app, log
from twitter.common.log.options import LogOptions
from twitter.common.zookeeper.serverset.endpoint import Endpoint, ServiceInstance
from twitter.mysos.common import zookeeper
from twitter.mysos.executor.executor import MysosExecutor
from twitter.mysos.executor.mysos_task_runner import MysosTaskRunner, TaskRunnerProvider
from twitter.mysos.executor.testing import FakeTaskControlProvider

import mesos.native
from zake.fake_client import FakeClient


class TestingTaskRunnerProvider(TaskRunnerProvider):
  """
    Creates a MysosTaskRunner with FakeTaskControl for testing purposes.
    NOTE: zake is used so a running ZK server is not required.
  """

  def __init__(self, task_control_provider):
    self._task_control_provider = task_control_provider

  def from_task(self, task):
    data = json.loads(task.data)
    cluster_name, port, zk_url = data['cluster'], data['port'], data['zk_url']

    _, servers, path = zookeeper.parse(zk_url)

    zk_client = FakeClient()
    zk_client.start()
    self_instance = ServiceInstance(Endpoint(socket.gethostbyname(socket.gethostname()), port))
    task_control = self._task_control_provider.from_task(task)

    return MysosTaskRunner(
        self_instance,
        zk_client,
        posixpath.join(path, cluster_name),
        task_control)


def main(args, options):
  log.info('Starting testing mysos executor')

  executor = MysosExecutor(TestingTaskRunnerProvider(FakeTaskControlProvider()))
  driver = mesos.native.MesosExecutorDriver(executor)
  driver.run()

  log.info('Exiting executor main')

LogOptions.disable_disk_logging()

# This is a testing executor. We log more verbosely.
LogOptions.set_stderr_log_level('google:DEBUG')
app.main()
