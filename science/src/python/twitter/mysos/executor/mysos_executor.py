import os
import stat

from twitter.common import app, log
from twitter.common.dirutil import safe_mkdir
from twitter.common.log.options import LogOptions

from .executor import MysosExecutor
from .mysql_task_control import MySQLTaskControlProvider
from .mysos_task_runner import MysosTaskRunnerProvider

import mesos.native
import pkg_resources


MYSOS_MODULE = 'twitter.mysos.executor'
ASSET_RELPATH = 'files'
# 'sandbox' directory resides under the working directory assigned by the Mesos slave.
SANDBOX_ROOT = os.path.join(os.path.realpath('.'), "sandbox")


def unpack_assets(asset_path):
  """
    Copy executor assets into the sandbox.

    NOTE: Assets under files/ within the pex are copied to the sandbox directory but the parent
          'files' directory is not recreated in the sandbox.
  """
  for asset in pkg_resources.resource_listdir(MYSOS_MODULE, asset_path):
    asset_target = os.path.join(os.path.relpath(asset_path, ASSET_RELPATH), asset)
    if pkg_resources.resource_isdir(MYSOS_MODULE, os.path.join(asset_path, asset)):
      safe_mkdir(os.path.join(SANDBOX_ROOT, asset_target))
      unpack_assets(os.path.join(asset_path, asset))
    else:
      output_file = os.path.join(SANDBOX_ROOT, asset_target)
      with open(output_file, 'wb') as fp:
        fp.write(pkg_resources.resource_string(
          MYSOS_MODULE, os.path.join(ASSET_RELPATH, asset_target)))
        # Make scripts executable.
        if output_file.endswith('.sh'):
          st = os.stat(output_file)
          os.chmod(output_file, st.st_mode | stat.S_IEXEC)


def main(args, options):
  unpack_assets(ASSET_RELPATH)  # Unpack files.

  log.info("Starting Mysos executor within sandbox %s" % SANDBOX_ROOT)
  executor = MysosExecutor(
      MysosTaskRunnerProvider(MySQLTaskControlProvider()),
      sandbox=SANDBOX_ROOT)
  driver = mesos.native.MesosExecutorDriver(executor)
  driver.run()

  log.info('Exiting executor main')

LogOptions.disable_disk_logging()
LogOptions.set_stderr_log_level('google:INFO')
app.main()
