import os
import random
import subprocess
import threading

from twitter.mysos.common.decorators import synchronized
from twitter.mysos.executor.task_control import TaskControl, TaskControlProvider


class FakeTaskControlProvider(TaskControlProvider):
  """
    An implementation of TaskControlProvider for testing.
  """

  def from_task(self, task, sandbox):
    return FakeTaskControl(position=random.randint(0, 100))


class FakeTaskControl(TaskControl):
  def __init__(
      self,
      mysqld="tail -f /dev/null",
      start_cmd=":",
      reparent_cmd=":",
      promote_cmd=":",
      get_log_position_cmd=":",
      position=1):
    """
      :param mysqld: The command that 'simulates' mysqld (and does nothing).
      :param *_cmd: The commands that are executed for the respective NopTaskControl operations.
      :param position: The 'mysqld' log position to return as the result of get_log_position().
    """
    self._lock = threading.Lock()
    self._mysqld = mysqld
    self._start_cmd = start_cmd,
    self._reparent_cmd = reparent_cmd,
    self._promote_cmd = promote_cmd,
    self._get_log_position_cmd = get_log_position_cmd
    self._position = position
    self._process = None

  @synchronized
  def start(self):
    if self._process:
      return

    self._process = subprocess.Popen(self._mysqld, shell=True, preexec_fn=os.setsid)
    subprocess.check_call(self._start_cmd, shell=True)
    return self._process

  @synchronized
  def reparent(self, master_host, master_port):
    subprocess.check_call(self._reparent_cmd, shell=True)

  @synchronized
  def promote(self):
    subprocess.check_call(self._promote_cmd, shell=True)

  @synchronized
  def get_log_position(self):
    subprocess.check_call(self._get_log_position_cmd, shell=True)
    return self._position
