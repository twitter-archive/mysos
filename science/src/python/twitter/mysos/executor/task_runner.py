from abc import abstractmethod

from twitter.common.lang import Interface


class TaskError(Exception):
  pass


class TaskRunner(Interface):
  pass


class TaskRunnerProvider(Interface):
  @abstractmethod
  def from_task(self, task, sandbox):
    """
      Factory method that creates a TaskRunner instance from 'task' (TaskInfo).
    """
    pass
