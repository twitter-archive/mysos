from abc import abstractmethod

from twitter.common.lang import Interface


class PackageInstaller(Interface):
  """
    Responsible for installing MySQL and dependent packages.

    NOTE: It is not responsible for installing DB instances.
  """

  class Error(Exception): pass

  @abstractmethod
  def install(self):
    """
      :return: The environment variables necessary for MySQL to execute.
      :rtype: dict
    """
    pass


class PackageInstallerProvider(Interface):
  @abstractmethod
  def from_task(self, task, sandbox):
    """
      Factory method that creates a PackageInstaller instance from 'task' (TaskInfo).

      :return: The PackageInstaller instance.
    """
    pass
