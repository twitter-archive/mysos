"""A testing Mysos runner binary to run MySQL tasks locally."""

from collections import namedtuple
import getpass
import json
import os
import random
import socket
import stat
import string
import tempfile

from twitter.common import app, log
from twitter.common.log.options import LogOptions
from twitter.mysos.common.pkgutil import unpack_assets
from twitter.mysos.executor.mysql_task_control import MySQLTaskControlProvider
from twitter.mysos.executor.mysos_task_runner import MysosTaskRunnerProvider
from twitter.mysos.executor.noop_installer import NoopPackageInstallerProvider
from twitter.mysos.executor.sandbox import Sandbox
from twitter.mysos.executor.twitter_backup import TwitterBackupStoreProvider
from twitter.mysos.executor.twitter_installer import TwitterPackageInstallerProvider


MYSOS_MODULE = 'twitter.mysos.executor'
ASSET_RELPATH = 'files'


app.add_option(
    '--port',
    dest='port',
    type='int',
    default=None,
    help='Port for the mysqld server')


app.add_option(
    '--framework_user',
    dest='framework_user',
    default=getpass.getuser(),
    help='The Unix user that Mysos executor runs as')


app.add_option(
    '--cluster_name',
    dest='cluster_name',
    help='Name of the cluster')


app.add_option(
    '--cluster_user',
    dest='cluster_user',
    help='The user account on MySQL server')


app.add_option(
    '--cluster_password',
    dest='cluster_password',
    default=None,
    help='Password for the MySQL cluster')


app.add_option(
    '--server_id',
    dest='server_id',
    type=int,
    default=1,
    help="The 'server_id' of the MySQL instance in the cluster")


app.add_option(
    '--zk_url',
    dest='zk_url',
    default=None,
    help='ZooKeeper URL for various Mysos operations, in the form of '
         '"zk://username:password@servers/path". The sub-directory <zk_url>/discover is used for '
         'communicating MySQL cluster information between Mysos scheduler and executors')


app.add_option(
    '--admin_keypath',
    dest='admin_keypath',
    default=None,
    help='The path to the key file with MySQL admin credentials on Mesos slaves')


app.add_option(
    '--installer_args',
    dest='installer_args',
    default=None,
    help='Arguments for MySQL installer directly passed along to and parsed by the installer. e.g.,'
         ' a serialized JSON string')


app.add_option(
    '--backup_store_args',
    dest='backup_store_args',
    default=None,
    help="Arguments for the store for MySQL backups. Its use and format are defined by the backup "
         "store implementation. e.g., It can be a serialized JSON string")


app.add_option(
    '--restore_backup_id',
    dest='restore_backup_id',
    default=None,
    help="The 'backup_id' of the backup to restore from. If None then Mysos starts an empty"
         "instance")


app.add_option(
    '--sandbox',
    dest='sandbox',
    default=None,
    help="Path to the sandbox. If not specified, a temp directory is created for it")


def chmod_scripts(path):
  """Make scripts executable."""
  if path.endswith('.sh'):
    st = os.stat(path)
    os.chmod(path, st.st_mode | stat.S_IEXEC)


def gen_password():
  """Return a randomly-generated password of 21 characters."""
  return ''.join(random.choice(
      string.ascii_uppercase +
      string.ascii_lowercase +
      string.digits) for _ in range(21))


def main(args, options):
  if not options.sandbox:
    sandbox_root = tempfile.mkdtemp()
    log.info("--sandbox not specified. Using '%s' instead" % sandbox_root)
  else:
    sandbox_root = options.sandbox

  if not options.cluster_password:
    cluster_password = gen_password()
    log.info("Using cluster password: %s" % cluster_password)
  else:
    cluster_password = options.cluster_password

  if not options.port:
    app.error('Must specify --port')

  if not options.zk_url:
    app.error('Must specify --zk_url')

  if not options.admin_keypath:
    app.error('Must specify --admin_keypath')

  if not options.cluster_user:
    app.error('Must specify --cluster_user')

  if not options.cluster_name:
    app.error('Must specify --cluster_name')

  if not options.server_id:
    app.error('Must specify --server_id')

  sandbox_root = os.path.abspath(sandbox_root)

  unpack_assets(sandbox_root, MYSOS_MODULE, ASSET_RELPATH, execute=chmod_scripts)

  log.info("Starting Mysos runner within sandbox %s" % sandbox_root)

  TaskInfo = namedtuple('TaskInfo', ['data'])
  task = TaskInfo(data=json.dumps(dict(
      framework_user=options.framework_user,
      host=socket.gethostname(),
      port=options.port,
      cluster=options.cluster_name,
      cluster_user=options.cluster_user,
      cluster_password=cluster_password,
      server_id=options.server_id,
      zk_url=options.zk_url,
      admin_keypath=options.admin_keypath,
      installer_args=options.installer_args,
      backup_store_args=options.backup_store_args,
      restore_backup_id=options.restore_backup_id)))

  runner = MysosTaskRunnerProvider(
      MySQLTaskControlProvider(),
      (TwitterPackageInstallerProvider()
       if options.installer_args else NoopPackageInstallerProvider()),
      TwitterBackupStoreProvider()).from_task(task, Sandbox(sandbox_root))

  try:
    runner.start()
    returncode = runner.join()
    # Regardless of the return code, if 'runner' terminates, it failed!
    app.error("Task process terminated with return code %s" % returncode)
  except Exception as e:
    log.info("Stopping the task process if it is running")
    runner.stop()
    app.error(str(e))


LogOptions.disable_disk_logging()
LogOptions.set_stderr_log_level('google:INFO')
app.main()
