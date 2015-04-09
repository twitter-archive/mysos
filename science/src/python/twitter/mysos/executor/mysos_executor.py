import os
import stat

from twitter.common import app, log
from twitter.common.log.options import LogOptions
from twitter.mysos.common.pkgutil import unpack_assets

from .executor import MysosExecutor
from .mysql_task_control import MySQLTaskControlProvider
from .mysos_task_runner import MysosTaskRunnerProvider
from .sandbox import Sandbox
from .twitter_backup import TwitterBackupStoreProvider
from .twitter_installer import TwitterPackageInstallerProvider

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

  log.info("Starting Mysos executor within sandbox %s" % sandbox_root)
  executor = MysosExecutor(
      MysosTaskRunnerProvider(
          MySQLTaskControlProvider(),
          TwitterPackageInstallerProvider(),
          TwitterBackupStoreProvider()),
      sandbox=Sandbox(sandbox_root))
  driver = mesos.native.MesosExecutorDriver(executor)
  driver.run()

  log.info('Exiting executor main')

LogOptions.disable_disk_logging()
LogOptions.set_stderr_log_level('google:INFO')
app.main()
