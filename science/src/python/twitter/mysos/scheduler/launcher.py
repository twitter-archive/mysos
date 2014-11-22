import json
import posixpath
import random
import threading

from twitter.common import log
from twitter.common.quantity import Amount, Time
from twitter.common.zookeeper.serverset.endpoint import Endpoint, ServiceInstance
from twitter.mysos.common.cluster import ClusterManager
from twitter.mysos.common import zookeeper

from .elector import MySQLMasterElector

import mesos.interface.mesos_pb2 as mesos_pb2


# TODO(jyx): Replace this when we start taking tasks from an HTTP API.
TASK_CPUS = 1
TASK_MEM = 128


class LauncherError(Exception): pass


class MySQLClusterLauncher(object):
  """
    Responsible for launching and maintaining a MySQL cluster.

    Thread-safety:
      The launcher is thread-safe. It uses a separate thread to wait for the election result and
      can launch a new election within that thread. All other public methods are called from the
      scheduler driver thread.
  """

  def __init__(
      self,
      zk_url,
      kazoo,
      cluster_name,
      num_nodes,
      executor_uri,
      executor_cmd,
      election_timeout,
      query_interval=Amount(1, Time.SECONDS)):
    """
      :param query_interval: See MySQLMasterElector. Use the default value for production and allow
                             tests to use a different value.
    """

    # Passed along to executors.
    self._zk_url = zk_url
    self._executor_uri = executor_uri
    self._executor_cmd = executor_cmd
    self._election_timeout = election_timeout

    # Used by the elector.
    self._query_interval = query_interval

    zk_root = zookeeper.parse(zk_url)[2]
    self._manager = ClusterManager(kazoo, posixpath.join(zk_root, cluster_name))
    self._cluster = MySQLCluster(cluster_name, num_nodes)

    self._driver = None  # Assigned by launch().

    self._lock = threading.Lock()
    self._elector = None  # The elector is set when the election starts. It is reset to None when it
                          # ends.

  @property
  def cluster_name(self):
    return self._cluster.name

  def launch(self, driver, offer):
    """
      Try to launch a MySQL task with the given offer.

      :returns:
        Task ID: Either the task ID of the task just launched or None if this offer is not used.
        Remaining resources: Resources from this offer that are unused by the task. If no task is
            launched, all the resources from the offer are returned.
    """
    if not self._driver:
      self._driver = driver

    with self._lock:
      if len(self._cluster.active_tasks) == self._cluster.num_nodes:
        # All nodes of this cluster have been launched and none have died.
        return None, offer.resources

      cpus, mem, ports = get_resources(offer.resources)

      # TODO(jyx): Replace the static resource requirements with what the user requests.
      task_cpus = TASK_CPUS
      task_mem = TASK_MEM

      if cpus < task_cpus or mem < task_mem or len(ports) == 0:
        # Offer doesn't fit.
        return None, offer.resources

      log.info("Launcher %s accepted offer %s on Mesos slave %s (%s)" % (
          self.cluster_name, offer.id.value, offer.slave_id.value, offer.hostname))

      task_port = random.choice(list(ports))  # Randomly pick a port in the offer.

      task = self._new_task(self._cluster, offer, task_cpus, task_mem, task_port)

      # TODO(bmahler): Re-launch if not running within a timeout.
      log.info('Launching task %s on Mesos slave %s (%s)' % (
          task.task_id.value, offer.slave_id.value, offer.hostname))

      self._cluster.tasks[task.task_id.value] = MySQLTask(
          self._cluster.name,
          task.task_id.value,
          task.slave_id.value,
          offer.hostname,
          task_port)

      # Update the offer's resources.
      remaining = create_resources(cpus - task_cpus, mem - task_mem, ports - set([task_port]))

      # Mysos launches at most a single task for each offer. Note that the SchedulerDriver API
      # expects a list of tasks.
      self._driver.launchTasks(offer.id, [task])

      return task.task_id.value, remaining

  def _new_task(self, cluster, offer, task_cpus, task_mem, task_port):
    """Return a new task with the requested resources."""
    task_id = self._new_task_id()

    task = mesos_pb2.TaskInfo()
    task.task_id.value = task_id
    task.slave_id.value = offer.slave_id.value
    task.name = task_id

    task.executor.executor_id.value = task_id  # Use task_id as executor_id.
    task.executor.command.value = self._executor_cmd

    uri = task.executor.command.uris.add()
    uri.value = self._executor_uri
    uri.executable = True
    uri.extract = False  # Don't need to decompress pex.

    task.data = json.dumps({
        'cluster': cluster.name,
        'port': task_port,
        'zk_url': self._zk_url
    })

    resources = create_resources(task_cpus, task_mem, set([task_port]))
    task.resources.extend(resources)

    return task

  def _new_task_id(self):
    task_id = "mysos-" + self.cluster_name + "-" + str(self._cluster.next_id)
    self._cluster.next_id += 1
    return task_id

  def _new_elector(self):
    """Create a new instance of MySQLMasterElector."""

    elector = MySQLMasterElector(
        self._driver,
        self.cluster_name,
        self._cluster.next_epoch,
        self._master_elected,
        self._election_timeout,
        query_interval=self._query_interval)

    log.info("Created elector for epoch %s for cluster %s" % (
        self._cluster.next_epoch, self.cluster_name))

    self._cluster.next_epoch += 1

    return elector

  def status_update(self, status):
    """
      Handle the status update for a task of this cluster.
    """
    with self._lock:
      task_id = status.task_id.value

      if task_id not in self._cluster.tasks:
        log.warn("Ignoring status update for unknown task %s" % task_id)
        return

      task = self._cluster.tasks[task_id]

      previous_state = task.state

      if previous_state == status.state:
        log.info('Ignoring duplicate status update %s for task %s' % (
            mesos_pb2.TaskState.Name(status.state),
            task_id))
        return

      if is_terminal(previous_state):
        log.info('Ignoring status update %s for task %s as it is in terminal state %s' % (
            mesos_pb2.TaskState.Name(status.state),
            task_id,
            mesos_pb2.TaskState.Name(previous_state)))
        return

      log.info('Updating state of task %s of cluster %s from %s to %s' % (
          status.task_id.value,
          task.cluster_name,
          mesos_pb2.TaskState.Name(previous_state),
          mesos_pb2.TaskState.Name(status.state)))

      task.state = status.state

      if task.state == mesos_pb2.TASK_RUNNING:
        # Register this cluster member.
        endpoint = Endpoint(task.hostname, task.port)
        member_id = self._manager.add_member(ServiceInstance(endpoint))
        self._cluster.members[task_id] = member_id
        log.info('Added %s (member id=%s) to cluster %s' % (endpoint, member_id, task.cluster_name))

        # If MySQL master is already elected for this cluster don't bother adding it to the elector.
        if self._cluster.master_id:
          log.info(
              "MySQL slave task %s on %s started after a master is already elected for cluster %s" %
              (task_id, task.hostname, self.cluster_name))
          return

        if not self._elector:
          self._elector = self._new_elector()
          # Add current slaves.
          for t in self._cluster.running_tasks:
            self._elector.add_slave(t.task_id, t.mesos_slave_id)
          self._elector.start()
        else:
          self._elector.add_slave(task_id, status.slave_id.value)
      elif task.state == mesos_pb2.TASK_FINISHED:
        raise LauncherError("Task %s is in unexpected state %s with message '%s'" % (
            status.task_id.value,
            mesos_pb2.TaskState.Name(status.state),
            status.message))
      elif is_terminal(task.state):
        log.error("Task %s is now in terminal state %s with message '%s'" % (
            status.task_id.value,
            mesos_pb2.TaskState.Name(status.state),
            status.message))

        if task_id in self._cluster.members:
          member_id = self._cluster.members[task_id]
          self._manager.remove_member(member_id)

          if member_id == self._cluster.master_id:
            log.info("Master of cluster %s has terminated. Restarting election" % self.cluster_name)

            assert not self._elector, "Election must not be running since there is a current master"
            self._elector = self._new_elector()
            # Add current slaves.
            for t in self._cluster.running_tasks:
              self._elector.add_slave(t.task_id, t.mesos_slave_id)
            self._elector.start()
          else:
            # It will be rescheduled next time the launcher is given an offer.
            log.info("Slave %s of cluster %s has terminated" % (task_id, self.cluster_name))
        else:
          assert previous_state != mesos_pb2.TASK_RUNNING, ("Task must exist in ClusterManager if "
              "it was running")
          log.warn("Slave %s of cluster %s failed to start running" % (task_id, self.cluster_name))

        self._cluster.remove_task(task_id)

  def _master_elected(self, master_task):
    """
      Invoked by the elector when a master is elected for this cluster.

      :param master_task: The task ID for the elected master.

      NOTE: A new election can be started if the currently elected master has already terminated
      before the election result arrives at the launcher.
    """
    if not master_task:
      log.error("No master can be elected for cluster %s" % self.cluster_name)
      return

    with self._lock:
      self._elector = None  # Elector will terminate soon.

      if master_task not in self._cluster.tasks:
        log.info("Slave %s of cluster %s was elected but has died. Restarting election" % (
            master_task, self.cluster_name))
        self._elector = self._new_elector()
        # Add current slaves.
        for t in self._cluster.running_tasks:
          self._elector.add_slave(t.task_id, t.mesos_slave_id)
        self._elector.start()
        return

      assert master_task in self._cluster.members, ("Elected master must have been added to "
          "'members'")
      member_id = self._cluster.members[master_task]
      slave_host = self._cluster.tasks[master_task].hostname

      log.info('Promoting MySQL task %s on host %s (member ID: %s) as the master for cluster %s' % (
          master_task, slave_host, member_id, self.cluster_name))
      self._manager.promote_member(member_id)
      self._cluster.master_id = member_id

  def framework_message(self, task_id, slave_id, message):
    with self._lock:
      if self._elector:
        data = json.loads(message)
        self._elector.update_position(int(data["epoch"]), task_id, data["position"])
      else:
        log.info("Received framework message '%s' from task %s (%s) when there is no pending "
            "election" % (message, task_id, slave_id))


# --- State classes. ---
class MySQLCluster(object):
  """
    The state of a MySQL cluster.

    It includes tasks (MySQLTask) for members of the cluster.
  """

  def __init__(self, name, num_nodes):
    # TODO(jyx): Consider encapsulating members with @properties but public members are OK for now.
    self.name = name
    self.num_nodes = num_nodes

    self.members = {}  # {TaskID : MemberID} mappings. MemberIDs are assigned by ZooKeeper.
    self.master_id = None  # MemberID of the MySQL master.
    self.tasks = {}  # {TaskID : MySQLTask} mappings
    self.next_epoch = 0  # Monotonically increasing number after each master change.
    self.next_id = 0  # Monotonically increasing number for unique task IDs.

  @property
  def active_tasks(self):
    """Tasks that have been launched and have not terminated."""
    return [t for t in self.tasks.values() if t.state in (
        mesos_pb2.TASK_STAGING, mesos_pb2.TASK_STARTING, mesos_pb2.TASK_RUNNING)]

  @property
  def running_tasks(self):
    return [t for t in self.tasks.values() if t.state == mesos_pb2.TASK_RUNNING]

  def remove_task(self, task_id):
    assert task_id in self.tasks
    del self.tasks[task_id]
    # A task may not be registered in 'members' (thus ZK) if it directly transitioned from STAGING
    # to a terminal state.
    if task_id in self.members:
      if self.members[task_id] == self.master_id:
        self.master_id = None
      del self.members[task_id]


class MySQLTask(object):
  """The state of a MySQL task."""

  def __init__(self, cluster_name, task_id, mesos_slave_id, hostname, port):
    self.cluster_name = cluster_name  # So we can refer back to the cluster it belongs to.
    self.task_id = task_id
    self.mesos_slave_id = mesos_slave_id
    self.hostname = hostname
    self.port = port
    self.state = mesos_pb2.TASK_STAGING  # Initial state. Will be updated by statusUpdate().


# --- Utility methods. ---
def get_resources(resources):
  """Return a tuple of the resources: cpus, mem, set of ports."""
  cpus, mem, ports = 0.0, 0, set()
  for resource in resources:
    if resource.name == 'cpus':
      cpus = resource.scalar.value
    elif resource.name == 'mem':
      mem = resource.scalar.value
    elif resource.name == 'ports' and resource.ranges.range:
      for r in resource.ranges.range:
        ports |= set(range(r.begin, r.end + 1))

  return cpus, mem, ports


def create_resources(cpus, mem, ports):
  """Return a list of 'Resource' protobuf for the provided resources."""
  cpus_resources = mesos_pb2.Resource()
  cpus_resources.name = 'cpus'
  cpus_resources.type = mesos_pb2.Value.SCALAR
  cpus_resources.scalar.value = cpus

  mem_resources = mesos_pb2.Resource()
  mem_resources.name = 'mem'
  mem_resources.type = mesos_pb2.Value.SCALAR
  mem_resources.scalar.value = mem

  ports_resources = mesos_pb2.Resource()
  ports_resources.name = 'ports'
  ports_resources.type = mesos_pb2.Value.RANGES
  for port in ports:
    port_range = ports_resources.ranges.range.add()
    port_range.begin = port
    port_range.end = port

  return [cpus_resources, mem_resources, ports_resources]


def is_terminal(state):
  """Return true if the task reached terminal state."""
  return state in [
    mesos_pb2.TASK_FINISHED,
    mesos_pb2.TASK_KILLED,
    mesos_pb2.TASK_FAILED,
    mesos_pb2.TASK_LOST]
