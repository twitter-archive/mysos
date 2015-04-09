import os
import json
import subprocess

from twitter.common import log

from .backup import BackupStore


BACKUP_INFO_FILENAME = "backup_info.json"


class StateManager(object):
  """
    Responsible for managing (restoring, initializing) the executor state.

    TODO(jyx): Will also periodically back up the state.
  """

  class Error(Exception): pass

  def __init__(self, sandbox, backup_store):
    """
      :param sandbox: The sandbox.
      :param backup_store: The BackupStore implementation.
    """
    self._sandbox = sandbox
    self._backup_store = backup_store

  def bootstrap(self, task_control, env):
    """
      Bootstraps the executor state.

      :param task_control: Task control for carrying out state bootstrapping commands.
      :param env: The environment variables for task_control methods.
    """

    # 1. Directly return if the data folder is not empty.
    if os.listdir(self._sandbox.mysql_data_dir):
      # TODO(jyx): This will be expected when we use persistent volumes. Validate the state in that
      # case.
      log.warn("MySQL state already exists unexpectedly. Finishing bootstrap without restoration "
               "or initialization")
      return

    # 2. If the data folder is clean, restore state from the backup store. This can be a noop if the
    # user doesn't want to restore any state.
    try:
      backup_info = self._backup_store.restore()
    except BackupStore.Error as e:
      raise self.Error("Failed to restore MySQL state: %s" % e)

    if backup_info:
      # If some backup is restored, persist the backup info.
      log.info("Finished restoring the backup")
      backup_info_file = os.path.join(self._sandbox.var, BACKUP_INFO_FILENAME)
      # Useful for the user to check the result of backup restore.
      with open(backup_info_file, 'w') as f:
        json.dump(backup_info.__dict__, f)
      log.info("Persisted backup info '%s' to file %s" % (backup_info.__dict__, backup_info_file))
    else:
      # If no recovery necessary, initialize the data dirs.
      log.info("No MySQL backup is restored. Initializing a new MySQL instance")
      try:
        task_control.initialize(env)
      except subprocess.CalledProcessError as e:
        raise self.Error("Unable to initialize MySQL state: %s" % e)
