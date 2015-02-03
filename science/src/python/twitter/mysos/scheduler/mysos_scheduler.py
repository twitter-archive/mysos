import os
import tempfile

from twitter.common import app, log
from twitter.common.exceptions import ExceptionalThread
from twitter.common.http import HttpServer
from twitter.common.log.options import LogOptions
from twitter.common.quantity.parse_simple import InvalidTime, parse_time
from twitter.mysos.common import zookeeper

from .http import MysosServer
from .scheduler import MysosScheduler
from .state import LocalStateProvider, Scheduler, StateProvider

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
    '--mysql_pkg_uri',
    dest='mysql_pkg_uri',
    default=None,
    help='URI for the MySQL package',
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


app.add_option(
    '--admin_keypath',
    dest='admin_keypath',
    default=None,
    help='The path to the key file with MySQL admin credentials on Mesos slaves',
)


app.add_option(
    '--work_dir',
    dest='work_dir',
    default=os.path.join(tempfile.gettempdir(), 'mysos'),
    help='Directory path to place Mysos work directories'
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

  if options.mysql_pkg_uri is None:
    app.error('Must specify --mysql_pkg_uri')

  if options.executor_cmd is None:
    app.error('Must specify --executor_cmd')

  if options.zk_url is None:
    app.error('Must specify --zk_url')

  if options.admin_keypath is None:
    app.error('Must specify --admin_keypath')

  try:
    election_timeout = parse_time(options.election_timeout)
  except InvalidTime as e:
    app.error(e.message)

  options.executor_uri = os.path.abspath(options.executor_uri)

  # Currently the scheduler state is persisted locally and can be restored upon local restarts.
  # TODO(jyx): Support failover.
  try:
    state_provider = LocalStateProvider(options.work_dir)
    state = state_provider.load_scheduler_state()
  except StateProvider.Error as e:
    app.error(e.message)

  if state:
    log.info("Successfully restored scheduler state")
    framework_info = state.framework_info
  else:
    log.info("No scheduler state to restore")
    framework_info = FrameworkInfo(
        user=options.framework_user,
        name=FRAMEWORK_NAME,
        checkpoint=True)
    state = Scheduler(framework_info)
    state_provider.dump_scheduler_state(state)

  try:
    _, zk_servers, zk_root = zookeeper.parse(options.zk_url)
  except Exception as e:
    app.error("Invalid --zk_url: %s" % e.message)

  kazoo = KazooClient(zk_servers)
  kazoo.start()

  scheduler = MysosScheduler(
      state,
      state_provider,
      options.framework_user,
      options.executor_uri,
      options.executor_cmd,
      kazoo,
      options.zk_url,
      election_timeout,
      options.admin_keypath,
      options.mysql_pkg_uri)

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
