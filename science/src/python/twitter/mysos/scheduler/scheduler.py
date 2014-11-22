from collections import OrderedDict
import random
import threading
import string

from twitter.common import log
from twitter.mysos.common import zookeeper
from twitter.mysos.common.cluster import get_cluster_path
from twitter.mysos.common.decorators import logged

from .launcher import LauncherError, MySQLClusterLauncher

import mesos.interface


class MysosScheduler(mesos.interface.Scheduler):

  class Error(Exception): pass
  class ClusterExists(Error): pass
  class InvalidUser(Error): pass

  def __init__(
      self,
      framework_user,
      executor_uri,
      executor_cmd,
      kazoo,
      zk_url,
      election_timeout,
      admin_keypath):
    """
      :param framework_user: The Unix user that Mysos executor runs as.
      :param executor_uri: URI for the Mysos executor pex file.
      :param executor_cmd: Command to launch the executor.
      :param kazoo: The Kazoo client for communicating MySQL cluster information between the
                    scheduler and the executors.
      :param zk_url: ZooKeeper URL for used by the scheduler and the executors to access ZooKeeper.
    """
    self._lock = threading.Lock()

    self._framework_user = framework_user
    self._executor_uri = executor_uri
    self._executor_cmd = executor_cmd
    self._election_timeout = election_timeout
    self._admin_keypath = admin_keypath

    self._driver = None  # Will be set by registered().

    self._zk_url = zk_url
    self._zk_root = zookeeper.parse(zk_url)[2]
    self._kazoo = kazoo

    self._launchers = OrderedDict()  # Order-preserving {cluster name : MySQLClusterLauncher}
                                     # mappings so cluster requests are fulfilled on a first come,
                                     # first serve (FCFS) basis.
    self._tasks = {}  # {TaskID : cluster_name} mappings.

    self.stopped = threading.Event()  # An event set when the scheduler is stopped.

  # --- Public interface. ---
  def create_cluster(self, cluster_name, cluster_user, num_nodes):
    """
      :param cluster_name: Name of the cluster.
      :param cluster_user: The user account on MySQL server.
      :param num_nodes: Number of nodes in the cluster.

      :return: a tuple of the following:
        - ZooKeeper URL for this Mysos cluster that can be used to resolve MySQL cluster info.
        - The password for the specified user of the specified cluster.

      TODO(jyx): We can expose the cluster-level ZooKeeper URL via an 'info' endpoint.
    """
    with self._lock:
      if cluster_name in self._launchers:
        raise self.ClusterExists("Cluster '%s' already exists" % cluster_name)

      if not cluster_user:
        raise self.InvalidUser('Invalid user name: %s' % cluster_user)

      if int(num_nodes) <= 0:
        raise ValueError("Invalid number of cluster nodes: %s" % num_nodes)

      cluster_password = gen_password()

      self._launchers[cluster_name] = MySQLClusterLauncher(
          self._zk_url,
          self._kazoo,
          self._framework_user,
          cluster_name,
          cluster_user,
          cluster_password,
          int(num_nodes),
          self._executor_uri,
          self._executor_cmd,
          self._election_timeout,
          self._admin_keypath)

      return get_cluster_path(self._zk_url, cluster_name), cluster_password

  def _stop(self):
    """Stop the scheduler."""
    self._driver.stop(True)  # Set failover to True.
    self.stopped.set()

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
      # Current scheduling algorithm: randomly pick an offer and loop through the list of launchers
      # until one decides to use this offer to launch a task.
      # It's possible to launch multiple tasks on the same Mesos slave (in different batches of
      # offers).
      for offer in shuffled(offers):
        task_id = None
        # For each offer, launchers are asked to launch a task in the order they are added (FCFS).
        for name in self._launchers:
          launcher = self._launchers[name]
          task_id, _ = launcher.launch(self._driver, offer)
          if task_id:
            self._tasks[task_id] = launcher.cluster_name
            # Move on to the next offer.
            break
        if not task_id:
          # No launcher can use this offer and we don't hoard offers.
          self._driver.declineOffer(offer.id)

  @logged
  def statusUpdate(self, driver, status):
    with self._lock:
      # Forward the status update to the corresponding launcher.
      task_id = status.task_id.value
      launcher = self._get_launcher_by_task_id(task_id)
      try:
        launcher.status_update(status)
      except LauncherError as e:
        log.error("Status update failed due to launcher error: %s" % e.message)
        self._stop()

  @logged
  def frameworkMessage(self, driver, executorId, slaveId, message):
    log.info('Received framework message %s' % message)
    task_id = executorId.value  # task_id == executor_id in Mysos.

    launcher = self._get_launcher_by_task_id(task_id)
    launcher.framework_message(task_id, slaveId.value, message)

  @logged
  def slaveLost(self, driver, slaveId):
    # We receive TASK_LOSTs when a slave is lost so we we don't need to handle it separately here.
    pass

  @logged
  def error(self, driver, message):
    log.error('Received error from mesos: %s' % message)
    self._stop()  # SchedulerDriver aborts when an error message is received.

  def _get_launcher_by_task_id(self, task_id):
    # TODO(jyx): Currently we don't delete entries from 'self._tasks' so a mapping can always be
    # found but we should clean it up when tasks die.
    assert task_id in self._tasks
    cluster_name = self._tasks[task_id]
    return self._launchers[cluster_name]


def shuffled(li):
  """Return a shuffled version of the list."""
  copy = li[:]
  random.shuffle(copy)
  return copy


def gen_password():
  """Return a randomly-generated password of 21 characters."""
  return ''.join(random.choice(
      string.ascii_uppercase +
      string.ascii_lowercase +
      string.digits) for _ in range(21))
