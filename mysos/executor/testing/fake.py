"""This module is for all fake implementations of things in Mysos executor for testing."""

import os
import random
import subprocess
import threading

from mysos.common.decorators import synchronized
from mysos.executor.task_control import TaskControl, TaskControlProvider


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
      initialize_cmd=":",
      start_cmd=":",
      reparent_cmd=":",
      promote_cmd=":",
      get_log_position_cmd=":",
      position=1):
    """
      :param mysqld: The command that 'simulates' mysqld (and does nothing).
      :param *_cmd: The commands that are executed for the respective FakeTaskControl operations.
      :param position: The 'mysqld' log position to return as the result of get_log_position().
    """
    self._lock = threading.Lock()
    self._mysqld = mysqld
    self._initialize_cmd = initialize_cmd
    self._start_cmd = start_cmd,
    self._reparent_cmd = reparent_cmd,
    self._promote_cmd = promote_cmd,
    self._get_log_position_cmd = get_log_position_cmd
    self._position = position
    self._process = None

  @synchronized
  def initialize(self, env):
    subprocess.check_call(self._initialize_cmd, shell=True)

  @synchronized
  def start(self, env=None):
    if self._process:
      return

    self._process = subprocess.Popen(self._mysqld, shell=True, preexec_fn=os.setpgrp)
    subprocess.check_call(self._start_cmd, shell=True)
    return self._process

  @synchronized
  def reparent(self, master_host, master_port, env=None):
    subprocess.check_call(self._reparent_cmd, shell=True)

  @synchronized
  def promote(self, env=None):
    subprocess.check_call(self._promote_cmd, shell=True)

  @synchronized
  def get_log_position(self, env=None):
    subprocess.check_call(self._get_log_position_cmd, shell=True)
    return self._position
