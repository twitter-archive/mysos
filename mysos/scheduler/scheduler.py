from collections import OrderedDict
import json
import posixpath
import random
import threading
import traceback
import sys

from mysos.common.cluster import get_cluster_path
from mysos.common.decorators import logged

from .launcher import MySQLClusterLauncher
from .password import gen_password, PasswordBox
from .state import MySQLCluster, Scheduler, StateProvider

import mesos.interface
import mesos.interface.mesos_pb2 as mesos_pb2
from twitter.common import log
from twitter.common.collections.orderedset import OrderedSet
from twitter.common.quantity import Amount, Data, Time
from twitter.common.quantity.parse_simple import InvalidData, parse_data


DEFAULT_TASK_CPUS = 1.0
DEFAULT_TASK_DISK = Amount(2, Data.GB)
DEFAULT_TASK_MEM = Amount(512, Data.MB)

# Refuse the offer for "eternity".
# NOTE: Using sys.maxint / 2 because sys.maxint causes rounding and precision loss when converted to
# double 'refuse_seconds' in the ProtoBuf and results in a negative duration on Mesos Master.
INCOMPATIBLE_ROLE_OFFER_REFUSE_DURATION = Amount(sys.maxint / 2, Time.NANOSECONDS)


class MysosScheduler(mesos.interface.Scheduler):

  class Error(Exception): pass

  class ClusterExists(Error): pass
  class ClusterNotFound(Error): pass
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
      scheduler_key,
      installer_args=None,
      backup_store_args=None,
      executor_environ=None,
      framework_role='*'):
    """
      :param state: The Scheduler object.
      :param state_provider: The StateProvider instance that the scheduler should use to
                             restore/persist states.
      :param framework_user: See flags.
      :param executor_uri: See flags.
      :param executor_cmd: See flags.
      :param framework_role: See flags.
      :param election_timeout: See flags.
      :param admin_keypath: See flags.
      :param scheduler_key: Scheduler uses it to encrypt cluster passwords.
      :param installer_args: See flags.
      :param backup_store_args: See flags.
      :param executor_environ: See flags.
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
    self._framework_role = framework_role
    self._election_timeout = election_timeout
    self._admin_keypath = admin_keypath
    self._installer_args = installer_args
    self._backup_store_args = backup_store_args
    self._executor_environ = executor_environ

    self._driver = None  # Will be set by registered().

    # Use a subdir to avoid name collision with the state storage.
    self._discover_zk_url = posixpath.join(zk_url, "discover")
    self._kazoo = kazoo

    self._scheduler_key = scheduler_key
    self._password_box = PasswordBox(scheduler_key)

    self._tasks = {}  # {Task ID: cluster name} mappings.
    self._launchers = OrderedDict()  # Order-preserving {cluster name : MySQLClusterLauncher}
                                     # mappings so cluster requests are fulfilled on a first come,
                                     # first serve (FCFS) basis.

    self.stopped = threading.Event()  # An event set when the scheduler is stopped.
    self.connected = threading.Event()  # An event set when the scheduler is first connected to
                                        # Mesos. The scheduler tolerates later disconnections.

  # --- Public interface. ---
  def create_cluster(self, cluster_name, cluster_user, num_nodes, size=None, backup_id=None):
    """
      :param cluster_name: Name of the cluster.
      :param cluster_user: The user account on MySQL server.
      :param num_nodes: Number of nodes in the cluster.
      :param size: The size of instances in the cluster as a JSON dictionary of 'cpus', 'mem',
                   'disk'. 'mem' and 'disk' are specified with data size units: kb, mb, gb, etc. If
                   given 'None' then app defaults are used.
      :param backup_id: The 'backup_id' of the backup to restore from. If None then Mysos starts an
                        empty instance.

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

      resources = parse_size(size)
      log.info("Requested resources per instance for cluster %s: %s" % (resources, cluster_name))

      self._state.clusters.add(cluster_name)
      self._state_provider.dump_scheduler_state(self._state)

      # Return the plaintext version to the client but store the encrypted version.
      password = gen_password()
      cluster = MySQLCluster(
          cluster_name,
          cluster_user,
          self._password_box.encrypt(password),
          int(num_nodes),
          cpus=resources['cpus'],
          mem=resources['mem'],
          disk=resources['disk'],
          backup_id=backup_id)
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
          self._scheduler_key,
          installer_args=self._installer_args,
          backup_store_args=self._backup_store_args,
          executor_environ=self._executor_environ,
          framework_role=self._framework_role)

      return get_cluster_path(self._discover_zk_url, cluster_name), password

  def delete_cluster(self, cluster_name, password):
    """
      :return: ZooKeeper URL for this Mysos cluster that can be used to wait for the termination of
               the cluster.
    """
    with self._lock:
      if not self._driver:
        raise self.ServiceUnavailable("Service unavailable. Try again later")

      if cluster_name not in self._state.clusters:
        raise self.ClusterNotFound("Cluster '%s' not found" % cluster_name)

      launcher = self._launchers[cluster_name]
      launcher.kill(password)
      log.info("Attempted to kill cluster %s" % cluster_name)

      return get_cluster_path(self._discover_zk_url, cluster_name)

  @property
  def clusters(self):
    """
      A generator for information about clusters.
    """
    with self._lock:
      for launcher in self._launchers.values():
        yield launcher.cluster_info

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

    # Recover only after the scheduler is connected because it needs '_driver' to be assigned. This
    # is blocking the scheduler driver thread but we do want further messages to be blocked until
    # the scheduler state is fully recovered.
    # TODO(jyx): If performance becomes an issue, we can also restore all the state data while the
    # driver is connecting and proceed to recover all the internal state objects after the driver is
    # connected.
    try:
      self._recover()
    except Exception as e:
      log.error("Stopping scheduler because: %s" % e)
      log.error(traceback.format_exc())
      self._stop()
      return

    self.connected.set()

  def _recover(self):
    """
      Recover launchers for existing clusters. A newly created scheduler has no launcher to recover.
      TODO(jyx): The recovery of clusters can potentially be parallelized.
    """
    for cluster_name in OrderedSet(self._state.clusters):  # Make a copy so we can remove dead
                                                           # entries while iterating the copy.
      log.info("Recovering launcher for cluster %s" % cluster_name)

      cluster = self._state_provider.load_cluster_state(cluster_name)
      if not cluster:
        # The scheduler could have failed over before creating the launcher. The user request
        # should have failed and there is no cluster state to restore.
        log.info("Skipping cluster %s because its state cannot be found" % cluster_name)
        self._state.clusters.remove(cluster_name)
        self._state_provider.dump_scheduler_state(self._state)
        continue

      for task_id in cluster.tasks:
        self._tasks[task_id] = cluster.name  # Reconstruct the 'tasks' map.

      # Order of launchers is preserved thanks to the OrderedSet.
      # For recovered launchers we use the currently specified --framework_role and
      # --executor_environ, etc., instead of saving it in cluster state so the change in flags can
      # be picked up by existing clusters.
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
          self._scheduler_key,
          self._installer_args,
          self._backup_store_args,
          self._executor_environ,
          self._framework_role)

    log.info("Recovered %s clusters" % len(self._launchers))

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
        # 'filters' is set when we need to create a special filter for incompatible roles.
        filters = None
        # For each offer, launchers are asked to launch a task in the order they are added (FCFS).
        for name in self._launchers:
          launcher = self._launchers[name]
          try:
            task_id, _ = launcher.launch(offer)
          except MySQLClusterLauncher.IncompatibleRoleError as e:
            # This "error" is not severe and we expect this to occur frequently only when Mysos
            # first joins the cluster. For a running Mesos cluster this should be somewhat rare
            # because we refuse the offer "forever".
            filters = mesos_pb2.Filters()
            filters.refuse_seconds = INCOMPATIBLE_ROLE_OFFER_REFUSE_DURATION.as_(Time.SECONDS)
            break  # No need to check with other launchers.
          if task_id:
            self._tasks[task_id] = launcher.cluster_name
            # No need to check with other launchers. 'filters' remains unset.
            break
        if task_id:
          break  # Some launcher has used this offer. Move on to the next one.

        if filters:
          log.info("Declining offer %s for %s because '%s'" % (
              offer.id.value, INCOMPATIBLE_ROLE_OFFER_REFUSE_DURATION, e))
        else:
          log.info("Declining offer %s because no launcher can use this offer" % offer.id.value)
          # Mesos scheduler Python binding doesn't deal with filters='None' properly.
          # See https://issues.apache.org/jira/browse/MESOS-2567.
          filters = mesos_pb2.Filters()
        self._driver.declineOffer(offer.id, filters)

  @logged
  def statusUpdate(self, driver, status):
    with self._lock:
      # Forward the status update to the corresponding launcher.
      task_id = status.task_id.value
      launcher = self._get_launcher_by_task_id(task_id)
      if not launcher:
        log.info("Cluster for task %s doesn't exist. It could have been removed" % task_id)
        return

      try:
        launcher.status_update(status)
      except MySQLClusterLauncher.Error as e:
        log.error("Status update failed due to launcher error: %s" % e.message)
        self._stop()

      if launcher.terminated:
        log.info("Deleting the launcher for cluster %s because the cluster has terminated" %
                 launcher.cluster_name)
        self._state.clusters.discard(launcher.cluster_name)
        self._state_provider.dump_scheduler_state(self._state)
        del self._launchers[launcher.cluster_name]

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
    return self._launchers.get(cluster_name)  # Cluster could have been removed.


def shuffled(li):
  """Return a shuffled version of the list."""
  copy = li[:]
  random.shuffle(copy)
  return copy


def parse_size(size):
  """Return the resources specified in 'size' as a dictionary."""
  if not size:
    resources = dict(cpus=DEFAULT_TASK_CPUS, mem=DEFAULT_TASK_MEM, disk=DEFAULT_TASK_DISK)
  else:
    # TODO(jyx): Simplify this using T-shirt sizing
    # (https://github.com/twitter/mysos/issues/14).
    try:
      resources_ = json.loads(size)
      resources = dict(
        cpus=resources_['cpus'],
        mem=parse_data(resources_['mem']),
        disk=parse_data(resources_['disk']))
    except (TypeError, KeyError, ValueError, InvalidData):
      raise ValueError("'size' should be a JSON dictionary with keys 'cpus', 'mem' and 'disk'")

  return resources
