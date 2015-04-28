import Queue
import json
import os
import signal
from subprocess import CalledProcessError
import sys
import threading

from twitter.common import log
from twitter.common.concurrent import defer
from twitter.common.zookeeper.serverset.endpoint import Endpoint, ServiceInstance
from twitter.mysos.common.cluster import ClusterListener, get_cluster_path
from twitter.mysos.common.zookeeper import parse

from .installer import PackageInstaller
from .state import StateManager
from .task_runner import TaskError, TaskRunner, TaskRunnerProvider
from .task_control import TaskControl

from kazoo.client import KazooClient


class MysosTaskRunnerProvider(TaskRunnerProvider):
  def __init__(self, task_control_provider, installer_provider, backup_store_provider):
    self._task_control_provider = task_control_provider
    self._installer_provider = installer_provider
    self._backup_store_provider = backup_store_provider

  def from_task(self, task, sandbox):
    data = json.loads(task.data)
    cluster_name, host, port, zk_url = data['cluster'], data['host'], data['port'], data['zk_url']
    _, servers, path = parse(zk_url)
    kazoo = KazooClient(servers)
    kazoo.start()
    self_instance = ServiceInstance(Endpoint(host, port))

    try:
      task_control = self._task_control_provider.from_task(task, sandbox)
      installer = self._installer_provider.from_task(task, sandbox)
      backup_store = self._backup_store_provider.from_task(task, sandbox)
    except (TaskControl.Error, PackageInstaller.Error) as e:
      raise TaskError(e.message)

    state_manager = StateManager(sandbox, backup_store)

    return MysosTaskRunner(
        self_instance,
        kazoo,
        get_cluster_path(path, cluster_name),
        installer,
        task_control,
        state_manager)


class MysosTaskRunner(TaskRunner):
  """
    A runner that manages the lifecycle of a MySQL task (through the provided 'task_control').

    The task is executed as a long-running process its return code can be obtained using 'join()'.

    Thread-safety:
      This class is accessed from the MysosExecutor thread (not the ExecutorDriver thread because
      MysosExecutor invokes operations asynchronously) and the ClusterListener thread and is
      thread-safe.

    TODO(jyx): Push the knowledge of the underlying subprocess down to the task control and stop the
               the subprocess using the task control.
  """

  def __init__(self, self_instance, kazoo, cluster_root, installer, task_control, state_manager):
    """
      :param self_instance: The local ServiceInstance associated with this task runner.
      :param kazoo: Kazoo client, it should be started before being passed in.
      :param cluster_root: The ZooKeeper root path for *this cluster*.
      :param installer: The PackageInstaller for MySQL.
      :param task_control: The TaskControl that interacts with the task process.
      :param state_manager: The StateManager for managing the executor state.
    """
    self._installer = installer
    self._env = None  # The environment variables for the 'task_control' commands. Set by the
                      # installer.

    self._task_control = task_control
    self._state_manager = state_manager

    self._lock = threading.Lock()
    self._popen = None  # The singleton task process started by '_task_control'.

    self._started = False  # Indicates whether start() has already been called.
    self._stopping = False  # Indicates whether stop() has already been called.
    self._exited = threading.Event()  # Set when the task process has exited.
    self._result = Queue.Queue()  # The returncode returned by the task process or an exception.

    # Public events and queue.
    self.promoted = threading.Event()
    self.demoted = threading.Event()
    self.master = Queue.Queue()  # Set when a master change is detected.

    self._listener = ClusterListener(
        kazoo,
        cluster_root,
        self_instance,
        promotion_callback=self._on_promote,
        demotion_callback=self._on_demote,
        master_callback=self._on_master_change)  # Listener started by start().

  # --- Public interface. ---
  def start(self):
    """
      Start the runner in a separate thread and wait for the task process to be forked.
    """
    with self._lock:
      if self._started:
        raise TaskError("Runner already started")
      self._started = True

      # Can potentially hold the lock for a long time but it's OK since the runner is not accessed
      # by multiple threads until after it's started; can be a noop as well, depending on the
      # installer implementation.
      try:
        # 1. Install the application.
        self._env = self._installer.install()
        log.info("Package installation completed. Resulting environment variables: %s" % self._env)

        # 2. Restore/initialize the application state.
        self._state_manager.bootstrap(self._task_control, self._env)
        log.info("Executor state fully bootstrapped")

        # 3. Start the task subprocess.
        # Store the process so we can kill it if necessary.
        self._popen = self._task_control.start(env=self._env)
        log.info("Task started in subprocess %s" % self._popen.pid)
        defer(self._wait)

        # 4. Start monitoring.
        # Only start listening to ZK events after the task subprocess has been successfully started.
        self._listener.start()
      except (PackageInstaller.Error, StateManager.Error, CalledProcessError) as e:
        raise TaskError("Failed to start MySQL task: %s" % e)

  def _wait(self):
    # Block until the subprocess exits and delivers the return code.
    self._result.put(self._popen.wait())

    # Notify stop() if it is waiting.
    self._exited.set()

  def stop(self, timeout=10):
    """
      Stop the runner and wait for its thread (and the sub-processes) to exit.

      :param timeout: The timeout that the process should die before a hard SIGKILL is issued
                      (SIGTERM is used initially).
      :return: True if an active runner is stopped, False if the runner is not started or already
               stopping/stopped.
    """
    if not self._started:
      log.warn("Cannot stop the runner because it's not started")
      return False

    if self._stopping:
      log.warn("The runner is already stopping/stopped")
      return False

    with self._lock:
      log.info("Stopping runner")
      self._stopping = True

      if not self._popen:
        log.info("The runner task did not start successfully so no need to kill it")
        return False

      try:
        log.info("Terminating process group: %s" % self._popen.pid)
        os.killpg(self._popen.pid, signal.SIGTERM)
      except OSError as e:
        log.info("The sub-processes are already terminated: %s" % e)
        return False

    log.info("Waiting for process to terminate due to SIGTERM")

    # Escalate to SIGKILL if SIGTERM is not sufficient.
    if not self._exited.wait(timeout=timeout):
      with self._lock:
        try:
          log.warn("Killing process group %s which failed to terminate cleanly within %s secs" %
                   (self._popen.pid, timeout))
          os.killpg(self._popen.pid, signal.SIGKILL)
        except OSError as e:
          log.info("The sub-processes are already terminated: %s" % e)
          return False

    log.info("Waiting for process to terminate due to SIGKILL")
    if not self._exited.wait(timeout=timeout):
      raise TaskError("Failed to kill process group %s" % self._popen.pid)

    return True

  def get_log_position(self):
    """
      Get the log position of the MySQL slave. Return None if it cannot be obtained.
    """
    try:
      log_position = self._task_control.get_log_position(env=self._env)
      return log_position
    except CalledProcessError as e:
      raise TaskError("Unable to get the slave's log position: %s" % e)

  def join(self):
    """
      Wait for the runner to terminate.

      :return: The return code of the subprocess. NOTE: A negative value -N indicates that the
               child was terminated by signal N (on Unix).

      :exception: The TaskError exception due to an error in task control operations.
    """
    # Using 'sys.maxint' makes this forever wait interruptible.
    result = self._result.get(True, sys.maxint)
    if isinstance(result, Exception):
      raise result
    else:
      return result

  # --- ClusterListener handlers. ---
  def _on_promote(self):
    self.promoted.set()
    if not self._exited.is_set():
      defer(self._promote)

  def _promote(self):
    try:
      self._task_control.promote(env=self._env)
    except CalledProcessError as e:
      self._result.put(TaskError("Failed to promote the slave: %s" % e))
      self.stop()

  def _on_demote(self):
    """
      Executor shuts itself down when demoted.
    """
    self.demoted.set()

    # Stop the runner asynchronously.
    if not self._exited.is_set():
      log.info("Shutting down runner because it is demoted.")
      # Call stop() asynchronously because this callback is invoked from the Kazoo thread which we
      # don't want to block.
      defer(self.stop)

  def _on_master_change(self, master):
    self.master.put(master)
    if not self._exited.is_set():
      defer(lambda: self._reparent(master))

  def _reparent(self, master):
    try:
      self._task_control.reparent(
          master.service_endpoint.host,
          master.service_endpoint.port,
          env=self._env)
    except CalledProcessError as e:
      self._result.put(TaskError("Failed to reparent the slave: %s" % e))
      self.stop()
