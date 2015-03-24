import os
import tempfile

from twitter.common import app, log
from twitter.common.exceptions import ExceptionalThread
from twitter.common.http import HttpServer
from twitter.common.log.options import LogOptions
from twitter.common.quantity import Time
from twitter.common.quantity.parse_simple import InvalidTime, parse_time
from twitter.mysos.common import pkgutil, zookeeper

from .http import MysosServer
from .scheduler import MysosScheduler
from .state import LocalStateProvider, Scheduler, StateProvider
from .zk_state import ZooKeeperStateProvider

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
    '--framework_role',
    dest='framework_role',
    default='*',
    help="The role that Mysos framework runs as. If set, Mysos only uses Mesos pool resources with "
         "that role. The default value '*' is what Mesos considers as the default role.\n"
         "NOTE: Mesos master needs to be configured to allow the specified role. See its --roles "
         "flag")


app.add_option(
    '--executor_uri',
    dest='executor_uri',
    default=None,
    help='URI for the Mysos executor package')


app.add_option(
    '--executor_cmd',
    dest='executor_cmd',
    default=None,
    help='Command to execute the executor package')


app.add_option(
    '--zk_url',
    dest='zk_url',
    default=None,
    help='ZooKeeper URL for various Mysos operations, in the form of '
         '"zk://username:password@servers/path". The sub-directory <zk_url>/discover is used for '
         'communicating MySQL cluster information between Mysos scheduler and executors')


# TODO(jyx): This could also be made a per-cluster configuration.
app.add_option(
    '--election_timeout',
    dest='election_timeout',
    default='60s',
    help='The amount of time the scheduler waits for all slaves to respond during a MySQL master '
         'election, e.g., 60s. After the timeout the master is elected from only the slaves that '
         'have responded')


app.add_option(
    '--admin_keypath',
    dest='admin_keypath',
    default=None,
    help='The path to the key file with MySQL admin credentials on Mesos slaves')


app.add_option(
    '--work_dir',
    dest='work_dir',
    default=os.path.join(tempfile.gettempdir(), 'mysos'),
    help="Directory path to place Mysos work directories, e.g., web assets, state files if "
         "--state_storage=local. Default to a system temp directory.")


app.add_option(
    '--state_storage',
    dest='state_storage',
    default='zk',
    help="Mechanism to persist scheduler state. Available options are 'zk' and 'local'. If 'zk' is "
         "chosen, the scheduler state is stored under <zk_url>/state; see --zk_url. Otherwise "
         "'local' is chosen and the state is persisted under <work_dir>/state; see --work_dir")


app.add_option(
    '--framework_failover_timeout',
    dest='framework_failover_timeout',
    default='14d',
    help='Time after which Mysos framework is considered deleted. This implies losing all tasks. '
         'SHOULD BE VERY HIGH')


app.add_option(
    '--installer_args',
    dest='installer_args',
    default=None,
    help='Arguments for MySQL installer directly passed along to and parsed by the installer. e.g.,'
         ' a serialized JSON string'
)


FRAMEWORK_NAME = 'mysos'
MYSOS_MODULE = 'twitter.mysos.scheduler'
ASSET_RELPATH = 'assets'


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

  if options.admin_keypath is None:
    app.error('Must specify --admin_keypath')

  try:
    election_timeout = parse_time(options.election_timeout)
    framework_failover_timeout = parse_time(options.framework_failover_timeout)
  except InvalidTime as e:
    app.error(e.message)

  try:
    _, zk_servers, zk_root = zookeeper.parse(options.zk_url)
  except Exception as e:
    app.error("Invalid --zk_url: %s" % e.message)

  web_assets_dir = os.path.join(options.work_dir, "web")
  pkgutil.unpack_assets(web_assets_dir, MYSOS_MODULE, ASSET_RELPATH)
  log.info("Extracted web assets into %s" % options.work_dir)

  log.info("Starting Mysos scheduler")

  kazoo = KazooClient(zk_servers)
  kazoo.start()

  if options.state_storage == 'zk':
    log.info("Using ZooKeeper (path: %s) for state storage" % zk_root)
    state_provider = ZooKeeperStateProvider(kazoo, zk_root)
  else:
    log.info("Using local disk for state storage")
    state_provider = LocalStateProvider(options.work_dir)

  try:
    state = state_provider.load_scheduler_state()
  except StateProvider.Error as e:
    app.error(e.message)

  if state:
    log.info("Successfully restored scheduler state")
    framework_info = state.framework_info
    if framework_info.HasField('id'):
      log.info("Recovered scheduler's FrameworkID is %s" % framework_info.id.value)
  else:
    log.info("No scheduler state to restore")
    framework_info = FrameworkInfo(
        user=options.framework_user,
        name=FRAMEWORK_NAME,
        checkpoint=True,
        failover_timeout=framework_failover_timeout.as_(Time.SECONDS),
        role=options.framework_role)
    state = Scheduler(framework_info)
    state_provider.dump_scheduler_state(state)

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
      options.installer_args,
      framework_role=options.framework_role)

  scheduler_driver = mesos.native.MesosSchedulerDriver(
      scheduler,
      framework_info,
      options.mesos_master)

  scheduler_driver.start()

  server = HttpServer()
  server.mount_routes(MysosServer(scheduler, web_assets_dir))

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
