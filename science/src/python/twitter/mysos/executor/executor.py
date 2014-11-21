import json

from twitter.common import log
from twitter.common.concurrent import defer
from twitter.mysos.common.decorators import logged

from .task_runner import TaskError

import mesos.interface
import mesos.interface.mesos_pb2


class MysosExecutor(mesos.interface.Executor):
  """
    MysosExecutor is a fine-grained executor, i.e., one executor executes a single task.
  """

  def __init__(self, runner_provider):
    """
      :param runner_provider: An implementation of TaskRunnerProvider.
    """
    self._runner_provider = runner_provider
    self._runner = None  # A singleton task runner created by launchTask().
    self._driver = None  # Assigned in registered().
    self._killed = False  # True if the executor's singleton task is killed by the scheduler.

  # --- Mesos methods. ---
  @logged
  def registered(self, driver, executorInfo, frameworkInfo, slaveInfo):
    log.info('Registered with slave: %s' % slaveInfo)
    self._driver = driver  # Cache the driver to kill later.

  @logged
  def reregistered(self, driver, slaveInfo):
    log.info('Reregistered with slave: %s' % slaveInfo)

  @logged
  def disconnected(self, driver):
    log.info("ExecutorDriver disconnected from Mesos slave")

  @logged
  def launchTask(self, driver, task):
    if self._runner:
      log.error("Executor allows only one task")
      update = mesos.interface.mesos_pb2.TaskStatus()
      update.state = mesos.interface.mesos_pb2.TASK_FAILED
      driver.sendStatusUpdate(update)
      return

    # Create the runner here in the driver thread so subsequent task launches are rejected.
    self._runner = self._runner_provider.from_task(task)

    # Run the task in a separate daemon thread.
    defer(lambda: self._run_task(task))

  def _run_task(self, task):
    assert self._runner, "_runner should be created before this method is called"
    self._runner.start()

    assert self._driver is not None

    log.info("Task runner for task %s started" % task.task_id)
    update = mesos.interface.mesos_pb2.TaskStatus()
    update.task_id.value = task.task_id.value
    update.state = mesos.interface.mesos_pb2.TASK_RUNNING
    self._driver.sendStatusUpdate(update)

    # Wait for the task's return code (when it terminates).
    try:
      returncode = self._runner.join()
      # Regardless of the return code, if '_runner' terminates, it failed!
      log.error("Task process terminated with return code %s" % returncode)
    except TaskError as e:
      log.error("Task terminated: %s" % e)

    if self._killed:
      update.state = mesos.interface.mesos_pb2.TASK_KILLED
    else:
      update.state = mesos.interface.mesos_pb2.TASK_FAILED

    self._driver.sendStatusUpdate(update)
    self._kill()

  @logged
  def frameworkMessage(self, driver, message):
    if not self._runner:
      log.info('Ignoring framework message because no task is running yet')
      return

    defer(lambda: self._framework_message(message))

  def _framework_message(self, message):
    master_epoch = message  # The log position request is for electing the master of this 'epoch'.
    try:
      position = self._runner.get_log_position()
      log.info('Obtained log position %s for epoch %s' % (position, master_epoch))

      assert self._driver

      # TODO(jyx): Define the message in ProtoBuf or Thrift.
      self._driver.sendFrameworkMessage(json.dumps({
          'epoch': master_epoch,  # Send the epoch back without parsing it.
          'position': position
      }))
    except TaskError as e:
      # Log the error and do not reply to the framework.
      log.error("Committing suicide due to failure to process framework message: %s" % e)
      self._kill()

  @logged
  def killTask(self, driver, taskId):
    # Killing the task also kills the executor because there is one task per executor.
    log.info("Asked to kill task %s" % taskId)
    self._killed = True
    self._kill()

  def _kill(self):
    if self._runner:
      self._runner.stop()  # It could be already stopped. If so, self._runner.stop() is a no-op.

    assert self._driver

    # TODO(jyx): Fix https://issues.apache.org/jira/browse/MESOS-243.
    self._driver.stop()

  @logged
  def shutdown(self, driver):
    log.info("Asked to shut down")
    self._killed = True
    self._kill()

  @logged
  def error(self, driver, message):
    log.error("Shutting down due to error: %s" % message)
    self._killed = True
    self._kill()
