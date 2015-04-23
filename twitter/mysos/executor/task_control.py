from abc import abstractmethod

from twitter.common.lang import Interface


class TaskControlProvider(Interface):
  @abstractmethod
  def from_task(self, task, sandbox):
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
  def initialize(self, env):
    """
      Initialize a new DB instance.

      :param env: The 'env' necessary for 'subprocess' to run the command.
    """
    pass

  @abstractmethod
  def start(self, env=None):
    """
    Start the task in a subprocess.

    :param env: The 'env' necessary for 'subprocess' to run the command.
    :return: A subprocess.Popen object that represents the leader of the process group that executes
             the task.
  """
    pass

  @abstractmethod
  def reparent(self, master_host, master_port, env=None):
    """
      Reparent the MySQL slave to the new master.

      :param env: The 'env' necessary for 'subprocess' to run the command.
    """
    pass

  @abstractmethod
  def promote(self, env=None):
    """
      Promote a slave to mastership.

      :param env: The 'env' necessary for 'subprocess' to run the command.
    """
    pass

  @abstractmethod
  def get_log_position(self, env=None):
    """
      Retrieve the log position from mysqld.

      :param env: The 'env' necessary for 'subprocess' to run the command.
      :return: The log position, None if it cannot be obtained.
    """
    pass
