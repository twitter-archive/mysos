import json
import posixpath
import random
import socket
import threading

from twitter.common import log
from twitter.common.zookeeper.serverset.endpoint import Endpoint, ServiceInstance
from twitter.mysos.common import zookeeper
from twitter.mysos.common.cluster import ClusterManager
from twitter.mysos.common.decorators import logged

import mesos.interface
import mesos.interface.mesos_pb2 as mesos_pb2


# TODO(jyx): Replace this when we start taking tasks from an HTTP API.
TASK_CPUS = 1
TASK_MEM = 128


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


def shuffled(li):
  """Return a shuffled version of the list."""
  copy = li[:]
  random.shuffle(copy)
  return copy


def is_terminal(state):
  """Return true if the task reached terminal state."""
  return state in frozenset([
      mesos_pb2.TASK_FINISHED,
      mesos_pb2.TASK_KILLED,
      mesos_pb2.TASK_FAILED,
      mesos_pb2.TASK_LOST])


class MySQLCluster(object):
  """The state of a MySQL cluster."""

  def __init__(self, name, manager, num_nodes):
    # TODO(jyx): Consider encapsulating members with @properties but public members are OK for now.
    self.name = name
    self.manager = manager

    self.num_nodes = num_nodes
    self.launched_tasks = 0  # Tasks that have been launched.
    self.running_tasks = 0  # Tasks in the TASK_RUNNING state.
    self.hostname = None

    self.members = {}  # {TaskID : MemberID} mappings.


class MySQLTask(object):
  """The state of a MySQL task."""

  def __init__(self, cluster_name, task_id, hostname, port):
    self.cluster_name = cluster_name
    self.task_id = task_id
    self.hostname = hostname
    self.port = port
    self.state = mesos_pb2.TASK_STAGING  # Initial state. Will be updated by statusUpdate().


class MysosScheduler(mesos.interface.Scheduler):

  class Error(Exception): pass
  class ClusterExists(Error): pass

  def __init__(self, framework_user, executor_uri, executor_cmd, kazoo, zk_url):
    """
    :param framework_user: The Unix user that Mysos executor runs as.
    :param executor_uri: URI for the Mysos executor pex file.
    :param executor_cmd: Command to launch the executor.
    :param kazoo: The Kazoo client for communicating MySQL cluster information between the scheduler
                  and the executors.
    :param zk_url: ZooKeeper URL for used by the scheduler and the executors to access ZooKeeper.
    """
    self._lock = threading.Lock()

    self._framework_user = framework_user
    self._executor_uri = executor_uri
    self._executor_cmd = executor_cmd

    self._driver = None  # Will be set by registered().

    self._zk_url = zk_url
    self._zk_root = zookeeper.parse(zk_url)[2]
    self._kazoo = kazoo

    self._clusters = {}  # {Name : MySQLCluster} mappings.
    self._tasks = {}  # {TaskID : MySQLTask} mappings.

    self._next_id = 0  # Next unique TaskID.

    self.stopped = threading.Event()  # An event set when the scheduler is stopped.

  def _new_task_id(self, cluster_name):
    task_id = "mysos-" + cluster_name + "-" + str(self._next_id)
    self._next_id += 1
    return task_id

  # --- Public interface. ---
  def create_cluster(self, name, num_nodes):
    """
    :param name: Name of the cluster.
    :param num_nodes: Number of nodes in the cluster.
    """

    with self._lock:
      if name in self._clusters:
        raise self.ClusterExists("Cluster '%s' already exists" % name)

      if int(num_nodes) <= 0:
        raise ValueError("Invalid number of cluster nodes: %s" % num_nodes)

      self._clusters[name] = MySQLCluster(
          name,
          ClusterManager(self._kazoo, posixpath.join(self._zk_root, name)),
          int(num_nodes))

  # --- Mesos methods. ---
  @logged
  def registered(self, driver, frameworkId, masterInfo):
    self._driver = driver

  @logged
  def reregistered(self, driver, masterInfo):
    pass

  @logged
  def disconnected(self, driver):
    pass

  @logged
  def resourceOffers(self, driver, offers):
    log.info('Got %d resource offers' % len(offers))

    with self._lock:
      # Current scheduling algorithm: randomly pick an offer and launch a task if it can fit in the
      # offer. It's possible to launch multiple tasks on the same Mesos slave.
      for offer in shuffled(offers):
        tasks = []

        for cluster in self._clusters.values():
          if cluster.launched_tasks == cluster.num_nodes:
            # All nodes of this cluster have been launched.
            continue

          cpus, mem, ports = get_resources(offer.resources)

          # TODO(jyx): Replace the static resource requirements with what the user requests.
          task_cpus = TASK_CPUS
          task_mem = TASK_MEM

          if cpus < task_cpus or mem < task_mem or len(ports) == 0:
            # TODO(jyx): We should only decline an offer if it doesn't fit the smallest MySQL
            # configuration (when we have multiple configurations that users can choose from).
            driver.declineOffer(offer.id)
            continue

          log.info("Accepting offer %s on %s" % (offer.id.value, offer.hostname))

          task_port = random.sample(ports, 1)[0]  # Randomly pick a port in the offer.

          task = self._new_task(cluster, offer, task_cpus, task_mem, task_port)

          # TODO(bmahler): Re-launch if not running within a timeout.
          log.info('Launching task %s on slave %s' % (task.task_id.value, offer.hostname))

          tasks.append(task)
          cluster.launched_tasks += 1

          self._tasks[task.task_id.value] = MySQLTask(
              cluster.name,
              task.task_id.value,
              offer.hostname,
              task_port)

          # Update the offer's resources.
          remaining = create_resources(cpus - task_cpus, mem - task_mem, ports)
          del offer.resources[:]
          offer.resources.extend(remaining)

        driver.launchTasks(offer.id, tasks)

  def _new_task(self, cluster, offer, task_cpus, task_mem, task_port):
    """Return a new task with the requested resources."""
    task_id = self._new_task_id(cluster.name)

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

  @logged
  def statusUpdate(self, driver, update):
    with self._lock:
      task_id = update.task_id.value
      task = self._tasks[task_id]
      cluster = self._clusters[task.cluster_name]
      previous_state = task.state

      if previous_state == update.state:
        log.info('Ignoring duplicate status update %s for task %s' % (
            mesos_pb2.TaskState.Name(update.state),
            task_id))
        return

      if is_terminal(previous_state):
        log.info('Ignoring status update %s for task %s as it is in terminal state %s' % (
            mesos_pb2.TaskState.Name(update.state),
            task_id,
            mesos_pb2.TaskState.Name(previous_state)))
        return

      log.info('Status update %s for task %s of cluster %s' % (
          mesos_pb2.TaskState.Name(update.state), update.task_id.value, task.cluster_name))

      if update.state == mesos_pb2.TASK_RUNNING:
        cluster.running_tasks += 1

        # Register this cluster member.
        endpoint = Endpoint(socket.gethostbyname(task.hostname), task.port)
        member = cluster.manager.add_member(ServiceInstance(endpoint))
        cluster.members[task_id] = member
        log.info('Added %s (member id=%s) to cluster %s' % (endpoint, member, task.cluster_name))

        # Current algorithm to pick the master: randomly pick one when all nodes have started.
        # TODO(jyx): Choose the most 'current' slave.
        if cluster.running_tasks == cluster.num_nodes:
          # All members have started.
          master = random.sample(cluster.members.values(), 1)[0]
          log.info("Promoting %s to mastership" % master)
          cluster.manager.promote_member(master)
      elif update.state == mesos_pb2.TASK_LOST:
        log.error("Task %s is lost with message '%s'" % (update.task_id.value, update.message))
        # TODO(jyx): Handle TASK_LOST by re-launching it and adjusting 'cluster.running_tasks'.
      elif is_terminal(update.state):
        log.error("Aborting because task %s is in unexpected state %s with message '%s'" % (
            update.task_id.value, mesos_pb2.TaskState.Name(update.state), update.message))
        # TODO(jyx): Re-launch instead of aborting if tasks failed to launch.
        driver.abort()

  @logged
  def frameworkMessage(self, driver, executorId, slaveId, message):
    pass

  @logged
  def slaveLost(self, driver, slaveId):
    # We receive TASK_LOSTs when a slave is lost so we we don't need to handle it separately here.
    pass

  @logged
  def error(self, driver, message):
    log.error('Received error from mesos: %s' % message)
    self.stopped.set()  # SchedulerDriver aborts when an error message is received.
