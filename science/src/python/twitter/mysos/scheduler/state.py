from abc import abstractmethod
import cPickle
from cPickle import PickleError
import os

from twitter.common import log
from twitter.common.collections.orderedset import OrderedSet
from twitter.common.dirutil import safe_mkdir

from mesos.interface import mesos_pb2


class StateProvider(object):
  """
    StateProvider is responsible for checkpointing and restoring the state of the Mysos scheduler.

    It maintains the following key hierarchy:
      /state/scheduler  # Scheduler-level state.
      /state/clusters/  # Folder for all cluster-level states.
        cluster1        # State for 'cluster1'.
        cluster2        # State for 'cluster2'.
        ...
  """

  class Error(Exception): pass

  @abstractmethod
  def dump_scheduler_state(self, state):
    """Persist scheduler-level state."""
    pass

  @abstractmethod
  def load_scheduler_state(self):
    """
      Restore scheduler-level state.
      :return: The Scheduler object. None if no state is available.
    """
    pass

  @abstractmethod
  def dump_cluster_state(self, state):
    """Persist cluster-level state."""
    pass

  @abstractmethod
  def load_cluster_state(self, cluster_name):
    """
      Restore cluster-level state.
      :return: The MySQLCluster object. None if no state is available.
    """
    pass

  # --- Helper methods. ---
  @classmethod
  def _get_scheduler_state_key(cls):
    return ['state', 'scheduler']

  @classmethod
  def _get_cluster_state_key(cls, cluster_name):
    return ['state', 'clusters', cluster_name]


class Scheduler(object):
  """
    Scheduler-level state.

    NOTE: It references cluster-level states indirectly through cluster names.
  """

  def __init__(self, framework_info):
    self.framework_info = framework_info
    self.clusters = OrderedSet()  # Names of clusters this scheduler manages. cluster creation
                                  # order is preserved with the OrderedSet.


class MySQLCluster(object):
  """
    The state of a MySQL cluster.

    It includes tasks (MySQLTask) for members of the cluster.
  """

  def __init__(self, name, user, password, num_nodes):
    if not isinstance(num_nodes, int):
      raise TypeError("'num_nodes' should be an int")

    self.name = name
    self.user = user
    self.password = password
    self.num_nodes = num_nodes

    self.members = {}  # {TaskID : MemberID} mappings. MemberIDs are assigned by ZooKeeper. A task
                       # must be running and published to ZK before it becomes a member.
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


class MySQLTask(object):
  """The state of a MySQL task."""

  def __init__(self, cluster_name, task_id, mesos_slave_id, hostname, port):
    if not isinstance(port, int):
      raise TypeError("'port' should be an int")

    self.cluster_name = cluster_name  # So we can refer back to the cluster it belongs to.
    self.task_id = task_id
    self.mesos_slave_id = mesos_slave_id
    self.hostname = hostname
    self.port = port
    self.state = mesos_pb2.TASK_STAGING  # Initial state. Will be updated by statusUpdate().


class LocalStateProvider(StateProvider):
  """StateProvider implementation that uses local disk to store the state."""

  def __init__(self, work_dir):
    """
      :param work_dir: The root directory under which the scheduler state is stored. e.g. The path
                       for 'cluster1' is <work_dir>/state/clusters/cluster1.
    """
    self._work_dir = work_dir

  def dump_scheduler_state(self, state):
    if not isinstance(state, Scheduler):
      raise TypeError("'state' should be an instance of Scheduler")
    path = self._get_scheduler_state_path()
    safe_mkdir(os.path.dirname(path))

    try:
      with open(path, 'wb') as f:
        cPickle.dump(state, f)
    except PickleError as e:
      raise self.Error('Failed to persist Scheduler: %s' % e)

  def load_scheduler_state(self):
    path = self._get_scheduler_state_path()
    if not os.path.isfile(path):
      log.info("No scheduler state found on path %s" % path)
      return None

    try:
      with open(path, 'rb') as f:
        return cPickle.load(f)
    except PickleError as e:
      raise self.Error('Failed to recover Scheduler: %s' % e)

  def dump_cluster_state(self, state):
    if not isinstance(state, MySQLCluster):
      raise TypeError("'state' should be an instance of MySQLCluster")

    path = self._get_cluster_state_path(state.name)
    safe_mkdir(os.path.dirname(path))

    try:
      with open(path, 'wb') as f:
        return cPickle.dump(state, f)
    except PickleError as e:
      raise self.Error('Failed to persist state for cluster %s: %s' % (state.name, e))

  def load_cluster_state(self, cluster_name):
    path = self._get_cluster_state_path(cluster_name)
    if not os.path.isfile(path):
      log.info("No cluster state found on path %s" % path)
      return None

    try:
      with open(path, 'rb') as f:
        return cPickle.load(f)
    except PickleError as e:
      raise self.Error('Failed to recover MySQLCluster: %s' % e)

  # --- Helper methods. ---
  def _get_scheduler_state_path(self):
    return os.path.join(self._work_dir, os.path.join(*self._get_scheduler_state_key()))

  def _get_cluster_state_path(self, cluster_name):
    return os.path.join(self._work_dir, os.path.join(*self._get_cluster_state_key(cluster_name)))
