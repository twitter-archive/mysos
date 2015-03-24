from abc import abstractmethod

from twitter.common.lang import Interface


class BackupInfo(object):
  def __init__(self, backup_file, cold_backup):
    self.backup_file = backup_file
    self.cold_backup = cold_backup


class BackupStore(Interface):
  """
    The storage for Mysos executor state backup (i.e. MySQL data, etc.).

    Thread-safety: The BackupStore implementation is not expected to be thread-safe and the caller
                   should be responsible for it.
  """

  class Error(Exception): pass
  class BackupNotFoundError(Error): pass

  @abstractmethod
  def restore(self, backup_id):
    """
      Restore the backup.

      :param backup_id: An identifier string for locating the backup. Its format is defined by the
                        implementation.
      :return: The BackupInfo object.
    """
    pass


class BackupStoreProvider(Interface):
  @abstractmethod
  def from_task(self, task, sandbox):
    """
      Factory method that creates a BackupStore instance from 'task' (TaskInfo).

      :return: The BackupStore instance.
    """
    pass
