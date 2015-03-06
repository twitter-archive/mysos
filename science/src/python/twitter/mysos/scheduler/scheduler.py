from collections import OrderedDict
import posixpath
import random
import threading
import string

from twitter.common import log
from twitter.common.collections.orderedset import OrderedSet
from twitter.mysos.common.cluster import get_cluster_path
from twitter.mysos.common.decorators import logged

from .launcher import MySQLClusterLauncher
from .state import MySQLCluster, Scheduler, StateProvider

import mesos.interface


class MysosScheduler(mesos.interface.Scheduler):

  class Error(Exception): pass

  class ClusterExists(Error): pass
  class InvalidUser(Error): pass
  class ServiceUnavailable(Error): pass

  def __init__(
      self,
      state,
      state_provider,
      framework_user,
      executor_uri,
      executor_cmd,
      kazoo,
      zk_url,
      election_timeout,
      admin_keypath,
      installer_args):
    """
      :param state: The Scheduler object.
      :param state_provider: The StateProvider instance that the scheduler should use to
                             restore/persist states.
      :param framework_user: See flags.
      :param executor_uri: See flags.
      :param executor_cmd: See flags.
      :param election_timeout: See flags.
      :param admin_keypath: See flags.
      :param installer_args: See flags.
      :param kazoo: The Kazoo client for communicating MySQL cluster information between the
                    scheduler and the executors.
      :param zk_url: ZooKeeper URL for used by the scheduler and the executors to access ZooKeeper.
    """
    self._lock = threading.Lock()

    if not isinstance(state, Scheduler):
      raise TypeError("'state' should be an instance of Scheduler")
    self._state = state

    if not isinstance(state_provider, StateProvider):
      raise TypeError("'state_provider' should be an instance of StateProvider")
    self._state_provider = state_provider

    self._framework_user = framework_user
    self._executor_uri = executor_uri
    self._executor_cmd = executor_cmd
    self._election_timeout = election_timeout
    self._admin_keypath = admin_keypath
    self._installer_args = installer_args

    self._driver = None  # Will be set by registered().

    # Use a subdir to avoid name collision with the state storage.
    self._discover_zk_url = posixpath.join(zk_url, "discover")
    self._kazoo = kazoo

    self._tasks = {}  # {Task ID: cluster name} mappings.
    self._launchers = OrderedDict()  # Order-preserving {cluster name : MySQLClusterLauncher}
                                     # mappings so cluster requests are fulfilled on a first come,
                                     # first serve (FCFS) basis.

    # Recover launchers for existing clusters. A newly created scheduler has no launcher to recover.
    # TODO(jyx): The recovery of clusters can potentially be parallelized.
    for cluster_name in OrderedSet(self._state.clusters):  # Make a copy so we can remove dead
                                                           # entries while iterating the copy.
      log.info("Recovering launcher for cluster %s" % cluster_name)
      try:
        cluster = state_provider.load_cluster_state(cluster_name)
        if not cluster:
          # The scheduler could have failed over before creating the launcher. The user request
          # should have failed and there is no cluster state to restore.
          log.info("Skipping cluster %s because its state cannot be found" % cluster_name)
          self._state.clusters.remove(cluster_name)
          self._state_provider.dump_scheduler_state(self._state)
          continue
        for task_id in cluster.tasks:
          self._tasks[task_id] = cluster.name  # Reconstruct the 'tasks' map.
        self._launchers[cluster.name] = MySQLClusterLauncher(
            self._driver,
            cluster,
            self._state_provider,
            self._discover_zk_url,
            self._kazoo,
            self._framework_user,
            self._executor_uri,
            self._executor_cmd,
            self._election_timeout,
            self._admin_keypath,
            self._installer_args)  # Order of launchers is preserved.
      except StateProvider.Error as e:
        raise self.Error("Failed to recover cluster: %s" % e.message)

    log.info("Recovered %s clusters" % len(self._launchers))

    self.stopped = threading.Event()  # An event set when the scheduler is stopped.
    self.connected = threading.Event()  # An event set when the scheduler is first connected to
                                        # Mesos. The scheduler tolerates later disconnections.

  # --- Public interface. ---
  def create_cluster(self, cluster_name, cluster_user, num_nodes):
    """
      :param cluster_name: Name of the cluster.
      :param cluster_user: The user account on MySQL server.
      :param num_nodes: Number of nodes in the cluster.

      :return: a tuple of the following:
        - ZooKeeper URL for this Mysos cluster that can be used to resolve MySQL cluster info.
        - The password for the specified user of the specified cluster.

      NOTE:
        If the scheduler fails over after the cluster state is checkpointed, the new scheduler
        instance will restore the state and continue to launch the cluster. TODO(jyx): Need to allow
        users to retrieve the cluster info (e.g. the passwords) afterwards in case the user request
        has failed but the cluster is successfully created eventually.
    """
    with self._lock:
      if not self._driver:
        # 'self._driver' is set in (re)registered() after which the scheduler becomes "available".
        # We need to get hold of the driver to recover the scheduler.
        raise self.ServiceUnavailable("Service unavailable. Try again later")

      if cluster_name in self._state.clusters:
        raise self.ClusterExists("Cluster '%s' already exists" % cluster_name)

      if not cluster_user:
        raise self.InvalidUser('Invalid user name: %s' % cluster_user)

      if int(num_nodes) <= 0:
        raise ValueError("Invalid number of cluster nodes: %s" % num_nodes)

      self._state.clusters.add(cluster_name)
      self._state_provider.dump_scheduler_state(self._state)

      cluster = MySQLCluster(cluster_name, cluster_user, gen_password(), int(num_nodes))
      self._state_provider.dump_cluster_state(cluster)

      log.info("Creating launcher for cluster %s" % cluster_name)
      self._launchers[cluster_name] = MySQLClusterLauncher(
          self._driver,
          cluster,
          self._state_provider,
          self._discover_zk_url,
          self._kazoo,
          self._framework_user,
          self._executor_uri,
          self._executor_cmd,
          self._election_timeout,
          self._admin_keypath,
          self._installer_args)

      return get_cluster_path(self._discover_zk_url, cluster_name), cluster.password

  def _stop(self):
    """Stop the scheduler."""

    # 'self._driver' is set in registered() so it could be None if the scheduler is asked to stop
    # before it is registered (e.g. an error is received).
    if self._driver:
      self._driver.stop(True)  # Set failover to True.
    self.stopped.set()

  # --- Mesos methods. ---
  @logged
  def registered(self, driver, frameworkId, masterInfo):
    self._driver = driver
    self._state.framework_info.id.value = frameworkId.value
    self._state_provider.dump_scheduler_state(self._state)
    self.connected.set()

  @logged
  def reregistered(self, driver, masterInfo):
    self._driver = driver
    self.connected.set()
    # TODO(jyx): Reconcile tasks.

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
          task_id, _ = launcher.launch(offer)
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
      except MySQLClusterLauncher.Error as e:
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
    # TODO(jyx): Currently we don't delete entries from 'self._tasks' so a mapping can always
    # be found but we should clean it up when tasks die.
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
