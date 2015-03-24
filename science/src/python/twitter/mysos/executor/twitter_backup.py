import os
from datetime import datetime, timedelta
import logging
import posixpath
import subprocess

from twitter.common import log
from twitter.common.fs.hdfs import HDFSHelper
from twitter.common.log.options import LogOptions
from twitter.common.string import ScanfParser

from .backup import BackupInfo, BackupStore
from .sandbox import Sandbox
from .shell_utils import decompress, decrypt, hdfs_cat, pv, untar


HADOOP_CONF_DIR = '/etc/hadoop/conf'

# Twitter specific defaults.
BACKUP_KEY = "/etc/twkeys/mysos/dba_backup/dba-backup.key"
HDFS_BACKUP_DIR = "/user"
MAX_ALLOWED_BACKUP_AGE = timedelta(days=3)


class TwitterBackupStore(BackupStore):
  def __init__(
      self,
      sandbox,
      hadoop_conf_dir=HADOOP_CONF_DIR,
      backup_key=BACKUP_KEY,
      hdfs_backup_dir=HDFS_BACKUP_DIR,
      max_allowed_backup_age=MAX_ALLOWED_BACKUP_AGE):
    """
      :param sandbox: The Sandbox object.
      :param hadoop_conf_dir: The conf dir for "hadoop --config".
      :param backup_key: The key to decrypt the backup if it is encrypted.
      :param hdfs_backup_dir: The directory on HDFS where the backups are stored.
      :param max_allowed_backup_age: Maximum age allowed for the "latest" backup when restoring from
                                     a backup. To override this limitation an explicit timestamp
                                     needs to be specified in 'backup_id' or a file system path
                                     should be used. See comments on restore().
    """
    if not isinstance(sandbox, Sandbox):
      raise TypeError("'sandbox' should be an instance of Sandbox")
    self._sandbox = sandbox

    self._hadoop_conf_dir = hadoop_conf_dir
    self._hdfs = HDFSHelper(self._hadoop_conf_dir)
    self._backup_key = backup_key
    self._hdfs_backup_dir = hdfs_backup_dir

    if not isinstance(max_allowed_backup_age, timedelta):
      raise TypeError("'max_allowed_backup_age' should be an instance of timedelta")
    self._max_allowed_backup_age = max_allowed_backup_age

  def restore(self, backup_id):
    """
      :param backup_id: An identifier string for locating the backup.

      The 'backup_id' string can be either a file system path (an absolute path starting with a '/')
      to the backup file without the extensions on the HDFS (e.g. if the backup file is
      '/user/mysos/foo/201503102000.tar.gz.enc', the id is '/user/mysos/foo/201503102000'), or a
      'reference' to a backup. The full reference is in the form of
      '{group}/{cluster}/{partition}:{timestamp}'. The parts preceding ':' is the reference to the
      DB.
        - group: The group that the cluster belongs to. e.g., dba, mysos.
        - cluster: The name of the DB cluster for the backup.
        - partition: The cluster partition. It is optional.
        - timestamp: The backups's timestamp in the format of %Y%m%d%H%M (See
                     https://docs.python.org/2/library/datetime.html). e.g., '201503102000'.
                     'latest' is a valid timestamp shorthand which references to the most recent
                     backup.
      NOTE: On HDFS the directories happen to be organized as {group}/{cluster}/{partition} but this
            is implementation detail that the user shouldn't rely on.
    """
    if not (os.listdir(self._sandbox.mysql_data_dir) == [] and
            os.listdir(self._sandbox.mysql_log_dir) == [] and
            os.listdir(self._sandbox.mysql_tmp_dir) == []):
      raise self.Error(
          "Cannot restore backup because the MySQL directories under %s are not empty" %
          self._sandbox.mysql_var)

    cmd_pipeline = []  # We need to run a sequence of commands connected by pipes.

    # 1. Get the base path for the backup on HDFS.
    hdfs_base_path = self._parse_backup_identifier(backup_id)

    # 2. Fetch the log file.
    backup_file_log = hdfs_base_path + ".logfile"
    log.info("Fetching log file %s" % backup_file_log)
    returncode = self._hdfs.copy_to_local(
        backup_file_log, os.path.join(self._sandbox.mysql_tmp_dir, "backup.log"))
    if returncode != 0:
      raise self.Error(
          "Failed to fetch backup file log with hadoop command return code: %s" % returncode)

    cold_backup = _contains(
        "Shutting down MySQL", os.path.join(self._sandbox.mysql_tmp_dir, "backup.log"))
    log.info("The backup is a %s" % ("cold backup" if cold_backup else "hot backup"))

    # 3. Fetch the backup file.
    backup_file_ = self._hdfs.ls(hdfs_base_path + ".tar.*")
    if len(backup_file_) == 0:
      raise self.BackupNotFoundError(
          "Backup not found on path: %s" % hdfs_base_path + ".tar.*")
    if len(backup_file_) > 1:
      log.warn("Found multiple backup files with the backup base path %s, selecting the first file")

    backup_file, backup_size = backup_file_[0]
    log.info("Selected backup file %s (size=%s)" % (backup_file, backup_size))

    cmd_pipeline.append(hdfs_cat(backup_file, self._hadoop_conf_dir))

    # Only monitor the progress when we want verbose logging.
    if LogOptions.stderr_log_level() == logging.DEBUG:
      cmd_pipeline.append(pv(backup_size))

    # 4. Process the fetching stream.
    backup_filename = posixpath.basename(backup_file)

    if backup_filename.startswith('.'):
      raise ValueError("Invalid backup file name: cannot be a hidden file")

    # Iterating extensions from right to left.
    for extension in reversed(backup_filename.split('.')[1:]):
      if extension == 'enc':
        log.info("Backup %s is encrypted" % backup_file)
        cmd_pipeline.append(decrypt(key_file=self._backup_key))
      elif extension in ('gz', 'bz', 'lzo'):
        cmd_pipeline.append(decompress(extension))
      elif extension == 'tar':
        cmd_pipeline.append(untar(self._sandbox.mysql_var))

    restore_cmd = " | ".join(cmd_pipeline)
    log.info("Executing restore command: %s" % restore_cmd)
    returncode = subprocess.call(restore_cmd, shell=True)
    if returncode != 0:
      raise self.Error("Failed to restore backup with return code: %s" % returncode)

    log.info("Finished restoring backup from %s on HDFS to %s. Now cleaning up some files" % (
        backup_file, self._sandbox.mysql_var))

    subprocess.check_call(
        "rm -fv %s" % os.path.join(self._sandbox.mysql_data_dir, "*.log"), shell=True)
    subprocess.check_call(
        "rm -fv %s" % os.path.join(self._sandbox.mysql_data_dir, "auto.cnf"), shell=True)
    subprocess.check_call(
        "rm -fv %s" % os.path.join(self._sandbox.mysql_log_dir, "general.log*"), shell=True)
    subprocess.check_call(
        "rm -fv %s" % os.path.join(self._sandbox.mysql_log_dir, "mysqld.pid"), shell=True)
    subprocess.check_call(
        "rm -fv %s" % os.path.join(self._sandbox.mysql_log_dir, "mysql-slow.log"), shell=True)
    subprocess.check_call(
        "rm -rfv %s" % os.path.join(self._sandbox.mysql_log_dir, "pt-*"), shell=True)

    if cold_backup:
      log.info("Removing mysql-relay-bin.* because the backup is a cold backup")
      subprocess.check_call(
          "rm -fv %s" % os.path.join(self._sandbox.mysql_log_dir, "mysql-relay-bin.*"), shell=True)

    # Clean up this file all the time. It appears that flushing logs does not clean it up properly.
    subprocess.check_call(
        "rm -rfv %s" % os.path.join(self._sandbox.mysql_data_dir, "relay-log.info"), shell=True)

    log.info("Restored backup is now clean")

    return BackupInfo(backup_file, cold_backup)

  def _parse_backup_identifier(self, backup_identifier):
    """
      :return: The backup's HDFS base path (without any extension).
    """
    if not isinstance(backup_identifier, str):
      raise TypeError("'backup_identifier' should be an instance of str")

    if backup_identifier.startswith('/'):
      if posixpath.splitext(backup_identifier)[1]:
        raise ValueError(
            "Invalid file path for 'backup_identifier': do not specify the file extensions")
      log.info("'backup_identifier' string '%s' is an HDFS path" % backup_identifier)
      return backup_identifier

    try:
      db_reference, timestamp_ = backup_identifier.split(':')
      db_reference = db_reference.strip(posixpath.sep)
      log.info("'backup_identifier' string %s is a backup reference" % backup_identifier)
    except ValueError:
      raise ValueError("Invalid 'recover_from' argument")

    db_dir = posixpath.join(self._hdfs_backup_dir, db_reference)  # Dir for all backups of this DB.

    # 'latest' is a special timestamp which we'll expand to the timestamp of the latest backup.
    if timestamp_ == 'latest':
      log.info("Searching for the latest backup for the specified DB")
      # A generator of the timestamps with invalid ones filtered out.
      try:
        timestamps = (
            _extract_timestamp(path)
            for path, _
            in self._hdfs.ls(
                posixpath.join(
                    db_dir, '%s-*.logfile' % _get_backup_file_prefix(db_reference)),
                is_dir=False)
            if _extract_timestamp(path))
      except self._hdfs.InternalError:
        timestamps = []

      if not timestamps:
        raise self.BackupNotFoundError(
            "No backup of a valid timestamp can be found under %s" % db_dir)

      timestamp = max(timestamps)  # We want the largest (latest) timestamp.

      backup_datetime = _parse_timestamp(timestamp)
      if datetime.utcnow() - backup_datetime > self._max_allowed_backup_age:
        raise ValueError(
            "The latest backup file (timestamp=%s) is older than %s, too old to be usable" % (
                timestamp, self._max_allowed_backup_age))
    else:
      backup_datetime = _parse_timestamp(timestamp_)
      if datetime.utcnow() - backup_datetime > self._max_allowed_backup_age:
        log.warn("Using a backup file (timestamp=%s) older than %s" % (
            timestamp_, self._max_allowed_backup_age))

      timestamp = timestamp_

    return posixpath.join(
        self._hdfs_backup_dir,
        db_reference,
        "%s-%s" % (_get_backup_file_prefix(db_reference), timestamp))


def _extract_timestamp(path):
  """
    Return the timestamp string encoded in the filename.

    :return: The timestamp. If the path has an invalid timestamp, None is returned.
  """
  _, filename = posixpath.split(path)
  try:
    fields = ScanfParser('%(db_ref)s-%(timestamp)s.%(extension)s').parse(filename)
  except ScanfParser.ParseError:
    log.info("Invalid timestamp encoding in filename for path: %s" % path)
    return None
  return fields.timestamp


def _parse_timestamp(ts):
  """
    :param ts: Timestamp in the format of %Y%m%d%H%M (See
               https://docs.python.org/2/library/datetime.html), e.g. '201503102000'.
    :return: The result datetime object.
  """
  return datetime.strptime(ts, "%Y%m%d%H%M")


def _contains(text, path):
  """Return True if the specified 'text' can be found in the file."""
  with open(path, 'r') as f:
    for line in f:
      if text in line:
        return True
  return False


def _get_backup_file_prefix(ref):
  """
    Transform DB reference to the corresponding backup file name.

    :param ref: DB reference that looks like this: '{role}/{name}/{partition}'.
    :return: Backup file name looks like this: '{name}-{partition}'. The full file name will have
             the timestamp and the extensions appended.

    So all this does is to remove the the role and replace the '/'s with '-'.
  """
  _, remainder = ref.split('/', 1)
  return remainder.replace('/', '-')
