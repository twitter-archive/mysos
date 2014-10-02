from abc import abstractmethod

from twitter.common.lang import Interface


class TaskControlProvider(Interface):
  @abstractmethod
  def from_task(self, task):
    pass


class TaskControl(Interface):
  """
    This class encapsulates commands that control the MySQL task process.

    Implementation NOTEs:
    1. These commands (methods) should run serially when accessed from multiple
       threads.
    2. This class doesn't capture subprocess.CalledProcessErrors thrown when underlying MySQL
       commands exit with a non-zero return code. The caller should handle it.
  """

  class Error(Exception):
    pass

  @abstractmethod
  def start(self):
    """
    Start the task in a subprocess.
    :return: A subprocess.Popen object that represents the leader of the process group that executes
             the task.
    """
    pass

  @abstractmethod
  def reparent(self, master_host, master_port):
    """
      Reparent the MySQL slave to the new master.
    """
    pass

  @abstractmethod
  def promote(self):
    """
      Promote a slave to mastership.
    """
    pass

  @abstractmethod
  def get_log_position(self):
    """
      Retrieve the log position from mysqld.
      :return: The log position.
    """
    pass
