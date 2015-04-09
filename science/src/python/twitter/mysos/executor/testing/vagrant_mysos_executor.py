"""This 'testing' executor is built to be run in the vagrant VM.

It is basically the same as the normal Mysos executor except that it doesn't rely on HDFS.
"""

import os
import stat

from twitter.common import app, log
from twitter.common.log.options import LogOptions
from twitter.mysos.common.pkgutil import unpack_assets
from twitter.mysos.executor.mysql_task_control import MySQLTaskControlProvider
from twitter.mysos.executor.executor import MysosExecutor
from twitter.mysos.executor.mysos_task_runner import MysosTaskRunnerProvider
from twitter.mysos.executor.noop_installer import NoopPackageInstallerProvider
from twitter.mysos.executor.sandbox import Sandbox
from twitter.mysos.executor.backup import NoopBackupStoreProvider

import mesos.native


MYSOS_MODULE = 'twitter.mysos.executor'
ASSET_RELPATH = 'files'


def chmod_scripts(path):
  """Make scripts executable."""
  if path.endswith('.sh'):
    st = os.stat(path)
    os.chmod(path, st.st_mode | stat.S_IEXEC)


def main(args, options):
  # 'sandbox' directory resides under the working directory assigned by the Mesos slave.
  sandbox_root = os.path.join(os.path.realpath('.'), "sandbox")

  unpack_assets(sandbox_root, MYSOS_MODULE, ASSET_RELPATH, execute=chmod_scripts)

  log.info("Starting Vagrant Mysos executor within sandbox %s" % sandbox_root)

  sandbox = Sandbox(sandbox_root)
  executor = MysosExecutor(
      MysosTaskRunnerProvider(
          MySQLTaskControlProvider(),
          NoopPackageInstallerProvider(),  # Do not install any package.
          NoopBackupStoreProvider()),  # Do not recover any state.
      sandbox)
  driver = mesos.native.MesosExecutorDriver(executor)
  driver.run()

  log.info('Exiting executor main')

LogOptions.disable_disk_logging()
LogOptions.set_stderr_log_level('google:INFO')
app.main()
