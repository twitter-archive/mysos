"""This executable is for testing the Mysos' MySQL HDFS backup restore feature."""

import json
import tempfile

from twitter.common import app, log
from twitter.common.log.options import LogOptions
from twitter.mysos.executor.sandbox import Sandbox
from twitter.mysos.executor.twitter_backup import BACKUP_KEY, HADOOP_CONF_DIR, TwitterBackupStore


app.add_option(
    '--sandbox',
    dest='sandbox',
    default=None,
    help="Path to the sandbox. If not specified, a temp directory is created for it")


app.add_option(
    '--backup_id',
    dest='backup_id',
    default=None,
    help="The database backup to restore from. See TwitterBackupStore.restore.__doc__: %s" %
         TwitterBackupStore.restore.__doc__)


app.add_option(
    '--hadoop_conf_dir',
    dest='hadoop_conf_dir',
    default=HADOOP_CONF_DIR,
    help="The config directory for HDFS commands")


app.add_option(
    '--backup_key',
    dest='backup_key',
    default=BACKUP_KEY,
    help="The key used to decrypt encrypted backup")


def main(args, options):
  if not options.restore_backup_id:
    app.error("Must specify --restore_backup_id")

  if not options.sandbox:
    sandbox_root = tempfile.mkdtemp()
    log.info("--sandbox not specified. Using '%s' instead" % sandbox_root)
  else:
    sandbox_root = options.sandbox

  try:
    backup_store = TwitterBackupStore(
        Sandbox(sandbox_root), options.backup_id, hadoop_conf_dir=options.hadoop_conf_dir)
    info = backup_store.restore()
    log.info(
        "Finished restoring the backup. Backup info: %s" % json.dumps(info.__dict__))
  except (ValueError, TwitterBackupStore.Error) as e:
    log.error("Failed to restore the backup: %s" % e)


LogOptions.disable_disk_logging()
LogOptions.set_stderr_log_level('google:DEBUG')
app.main()
