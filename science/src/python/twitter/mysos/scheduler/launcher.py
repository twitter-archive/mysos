import json
import posixpath
import random

from twitter.common import log
from twitter.common.zookeeper.serverset.endpoint import Endpoint, ServiceInstance
from twitter.mysos.common.cluster import ClusterManager
from twitter.mysos.common import zookeeper

import mesos.interface.mesos_pb2 as mesos_pb2


# TODO(jyx): Replace this when we start taking tasks from an HTTP API.
TASK_CPUS = 1
TASK_MEM = 128


class LauncherError(Exception): pass


class MySQLClusterLauncher(object):
  """
    Responsible for launching and maintaining a MySQL cluster.
  """

  def __init__(self, zk_url, kazoo, cluster_name, num_nodes, executor_uri, executor_cmd):
    # Passed along to executors.
    self._zk_url = zk_url
    self._executor_uri = executor_uri
    self._executor_cmd = executor_cmd

    zk_root = zookeeper.parse(zk_url)[2]
    self._manager = ClusterManager(kazoo, posixpath.join(zk_root, cluster_name))
    self._cluster = MySQLCluster(cluster_name, num_nodes)

    self._next_id = 0  # Next unique TaskID.
    self._driver = None  # Assigned by launch().

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

    if self._cluster.launched_tasks == self._cluster.num_nodes:
      # All nodes of this cluster have been launched.
      return None, offer.resources

    cpus, mem, ports = get_resources(offer.resources)

    # TODO(jyx): Replace the static resource requirements with what the user requests.
    task_cpus = TASK_CPUS
    task_mem = TASK_MEM

    if cpus < task_cpus or mem < task_mem or len(ports) == 0:
      # Offer doesn't fit.
      return None, offer.resources

    log.info("Launcher %s accepted offer %s on slave %s (%s)" % (
        self.cluster_name, offer.id.value, offer.slave_id.value, offer.hostname))

    task_port = random.sample(ports, 1)[0]  # Randomly pick a port in the offer.

    task = self._new_task(self._cluster, offer, task_cpus, task_mem, task_port)

    # TODO(bmahler): Re-launch if not running within a timeout.
    log.info('Launching task %s on slave %s (%s)' % (
        task.task_id.value, offer.slave_id.value, offer.hostname))

    self._cluster.launched_tasks += 1

    self._cluster.tasks[task.task_id.value] = MySQLTask(
        self._cluster.name,
        task.task_id.value,
        task.slave_id.value,
        offer.hostname,
        task_port)

    # Update the offer's resources.
    remaining = create_resources(cpus - task_cpus, mem - task_mem, ports - set([task_port]))

    # Mysos launches at most a single task for each offer. Note that the SchedulerDriver API accepts
    # a list of tasks.
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
    task_id = "mysos-" + self.cluster_name + "-" + str(self._next_id)
    self._next_id += 1
    return task_id

  def status_update(self, status):
    """
      Handle the status update for a task of this cluster.
    """
    task_id = status.task_id.value

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

    log.info('Status update %s for task %s of cluster %s' % (
        mesos_pb2.TaskState.Name(status.state),
        status.task_id.value,
        task.cluster_name))

    if status.state == mesos_pb2.TASK_RUNNING:
      self._cluster.running_tasks += 1

      # Register this cluster member.
      endpoint = Endpoint(task.hostname, task.port)
      member = self._manager.add_member(ServiceInstance(endpoint))
      self._cluster.members[task_id] = member
      log.info('Added %s (member id=%s) to cluster %s' % (endpoint, member, task.cluster_name))

      # Current algorithm to pick the master: randomly pick one when all nodes have started.
      # TODO(jyx): Choose the most 'current' slave.
      if self._cluster.running_tasks == self._cluster.num_nodes:
        # All members have started.
        master_id = random.sample(self._cluster.members.values(), 1)[0]
        log.info("Promoting %s to mastership" % master_id)
        self._manager.promote_member(master_id)
        # TODO(jyx): Handle with ZK failures.
        self._cluster.master_id = master_id
    elif status.state == mesos_pb2.TASK_FINISHED:
      raise LauncherError("Task %s is in unexpected state %s with message '%s'" % (
          status.task_id.value,
          mesos_pb2.TaskState.Name(status.state),
          status.message))
    elif is_terminal(status.state):
      log.error("Task %s enters terminal state %s with message '%s'" % (
          status.task_id.value,
          mesos_pb2.TaskState.Name(status.state),
          status.message))
      # TODO(jyx): Re-launch if tasks failed to launch or are lost. Also handle MySQL master
      # failover.
      self._cluster.running_tasks -= 1


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

    self.launched_tasks = 0  # Tasks that have been launched.
    self.running_tasks = 0  # Tasks in the TASK_RUNNING state.

    self.members = {}  # {TaskID : MemberID} mappings. MemberIDs are assigned by ZooKeeper.
    self.master_id = None  # MemberID of the MySQL master.
    self.tasks = {}  # {TaskID : MySQLTask} mappings


class MySQLTask(object):
  """The state of a MySQL task."""

  def __init__(self, cluster_name, task_id, slave_id, hostname, port):
    self.cluster_name = cluster_name  # So we can refer back to the cluster it belongs to.
    self.task_id = task_id
    self.slave_id = slave_id
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
