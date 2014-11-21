import os

from twitter.common import app, log
from twitter.common.exceptions import ExceptionalThread
from twitter.common.http import HttpServer
from twitter.common.log.options import LogOptions
from twitter.common.quantity.parse_simple import InvalidTime, parse_time
from twitter.mysos.common import zookeeper

from .scheduler import MysosScheduler
from .http import MysosServer

from kazoo.client import KazooClient

import mesos.interface
from mesos.interface.mesos_pb2 import FrameworkInfo
import mesos.native


app.add_option(
    '--port',
    dest='api_port',
    type='int',
    default=None,
    help='Port for the HTTP API server')


app.add_option(
    '--mesos_master',
    dest='mesos_master',
    default=None,
    help='Mesos master address. It can be a ZooKeeper URL through which the master can be detected')


app.add_option(
    '--framework_user',
    dest='framework_user',
    help='The Unix user that Mysos executor runs as')


app.add_option(
    '--executor_uri',
    dest='executor_uri',
    default=None,
    help='URI for the Mysos executor package',
)


app.add_option(
    '--executor_cmd',
    dest='executor_cmd',
    default=None,
    help='Command to execute the executor package',
)


app.add_option(
    '--zk_url',
    dest='zk_url',
    default=None,
    help='ZooKeeper URL for communicating MySQL cluster information between Mysos scheduler and '
         'executors',
)


# TODO(jyx): This could also be made a per-cluster configuration.
app.add_option(
    '--election_timeout',
    dest='election_timeout',
    default='60s',
    help='The amount of time the scheduler waits for all slaves to respond during a MySQL master '
         'election, e.g., 60s. After the timeout the master is elected from only the slaves that '
         'have responded',
)


FRAMEWORK_NAME = 'mysos'


def main(args, options):
  if options.api_port is None:
    app.error('Must specify --port')

  if options.mesos_master is None:
    app.error('Must specify --mesos_master')

  if options.framework_user is None:
    app.error('Must specify --framework_user')

  if options.executor_uri is None:
    app.error('Must specify --executor_uri')

  if options.executor_cmd is None:
    app.error('Must specify --executor_cmd')

  if options.zk_url is None:
    app.error('Must specify --zk_url')

  try:
    election_timeout = parse_time(options.election_timeout)
  except InvalidTime as e:
    app.error(e.message)

  options.executor_uri = os.path.abspath(options.executor_uri)

  # TODO(jyx): Support failover.
  framework_info = FrameworkInfo(
      user=options.framework_user,
      name=FRAMEWORK_NAME,
      checkpoint=True)

  try:
    _, zk_servers, zk_root = zookeeper.parse(options.zk_url)
  except Exception as e:
    app.error("Invalid --zk_url: %s" % e.message)

  kazoo = KazooClient(zk_servers)
  kazoo.start()

  # TODO(jyx): Should not do this once scheduler failover is implemented.
  kazoo.delete(zk_root, recursive=True)

  scheduler = MysosScheduler(
      options.framework_user,
      options.executor_uri,
      options.executor_cmd,
      kazoo,
      options.zk_url,
      election_timeout)

  scheduler_driver = mesos.native.MesosSchedulerDriver(
    scheduler,
    framework_info,
    options.mesos_master)

  scheduler_driver.start()

  server = HttpServer()
  server.mount_routes(MysosServer(scheduler))

  et = ExceptionalThread(
      target=server.run, args=('0.0.0.0', options.api_port, 'cherrypy'))
  et.daemon = True
  et.start()

  try:
    # Wait for the scheduler to stop.
    # The use of 'stopped' event instead of scheduler_driver.join() is necessary to stop the process
    # with SIGINT.
    while not scheduler.stopped.wait(timeout=0.5):
      pass
  except KeyboardInterrupt:
    log.info('Interrupted, exiting.')
  else:
    log.info('Scheduler exited.')

  app.shutdown(1)  # Mysos scheduler is supposed to be long-running thus the use of exit status 1.

LogOptions.disable_disk_logging()
LogOptions.set_stderr_log_level('google:INFO')

app.main()
