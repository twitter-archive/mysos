from collections import namedtuple
import json
import random
import threading

from mysos.common.cluster import ClusterManager, get_cluster_path
from mysos.common import zookeeper

from .elector import MySQLMasterElector
from .state import MySQLCluster, MySQLTask, StateProvider
from .password import PasswordBox

import mesos.interface.mesos_pb2 as mesos_pb2
from twitter.common import log
from twitter.common.quantity import Amount, Data, Time
from twitter.common.zookeeper.serverset.endpoint import Endpoint, ServiceInstance


class MySQLClusterLauncher(object):
  """
    Responsible for launching and maintaining a MySQL cluster.

    Thread-safety:
      The launcher is thread-safe. It uses a separate thread to wait for the election result and
      can launch a new election within that thread. All other public methods are called from the
      scheduler driver thread and the web UI threads.
  """

  class Error(Exception): pass
  class IncompatibleRoleError(Error): pass
  class PermissionError(Error): pass

  def __init__(
      self,
      driver,
      cluster,
      state_provider,
      zk_url,
      kazoo,
      framework_user,
      executor_uri,
      executor_cmd,
      election_timeout,
      admin_keypath,
      scheduler_key,
      installer_args=None,
      backup_store_args=None,
      executor_environ=None,
      framework_role='*',
      query_interval=Amount(1, Time.SECONDS)):
    """
      :param driver: Mesos scheduler driver.
      :param cluster: The MySQLCluster state object.
      :param state_provider: For restoring and persisting the cluster state.
      :param zk_url: The ZooKeeper URL for cluster member discovery and master election.
      :param kazoo: The Kazoo client to access ZooKeeper with.
      :param executor_uri: See flags.
      :param executor_cmd: See flags.
      :param election_timeout: See flags.
      :param admin_keypath: See flags.
      :param scheduler_key: Used for encrypting cluster passwords.
      :param installer_args: See flags.
      :param backup_store_args: See flags.
      :param executor_environ: See flags.
      :param framework_role: See flags.
      :param query_interval: See MySQLMasterElector. Use the default value for production and allow
                             tests to use a different value.
    """
    self._driver = driver

    if not isinstance(cluster, MySQLCluster):
      raise TypeError("'cluster' should be an instance of MySQLCluster")
    self._cluster = cluster

    if not isinstance(state_provider, StateProvider):
      raise TypeError("'state_provider' should be an instance of StateProvider")
    self._state_provider = state_provider

    self._framework_role = framework_role

    # Passed along to executors.
    self._zk_url = zk_url
    self._framework_user = framework_user
    self._executor_uri = executor_uri
    self._executor_cmd = executor_cmd
    self._election_timeout = election_timeout
    self._admin_keypath = admin_keypath
    self._installer_args = installer_args
    self._backup_store_args = backup_store_args
    self._executor_environ = executor_environ

    # Used by the elector.
    self._query_interval = query_interval

    zk_root = zookeeper.parse(zk_url)[2]
    self._cluster_manager = ClusterManager(kazoo, get_cluster_path(zk_root, cluster.name))

    self._password_box = PasswordBox(scheduler_key)
    self._password_box.decrypt(cluster.encrypted_password)  # Validate the password.

    self._lock = threading.Lock()

    if self._cluster.master_id:
      log.info("Republish master %s for cluster %s in case it's not published" % (
          self._cluster.master_id, self.cluster_name))
      self._cluster_manager.promote_member(self._cluster.master_id)

    if len(self._cluster.tasks) > 0:
      log.info("Recovered %s tasks for cluster '%s'" % (
          len(self._cluster.tasks), self.cluster_name))

    # A recovered launcher should continue the election if the previous one was incomplete when the
    # scheduler failed over. Mesos will deliver all missed events that affect the election to the
    # scheduler.
    if len(self._cluster.running_tasks) > 0 and not self._cluster.master_id:
      log.info("Restarting election for the recovered launcher")
      self._elector = self._new_elector()
      # Add current slaves.
      for t in self._cluster.running_tasks:
        self._elector.add_slave(t.task_id, t.mesos_slave_id)
      self._elector.start()
    else:
      # New launcher, the elector is set when the election starts and reset to None when it ends.
      self._elector = None

    self._terminating = False

  @property
  def cluster_name(self):
    return self._cluster.name

  @property
  def cluster_info(self):
    with self._lock:
      ClusterInfo = namedtuple('ClusterInfo', ('name, user, num_nodes'))
      return ClusterInfo(
          name=self._cluster.name, user=self._cluster.user, num_nodes=self._cluster.num_nodes)

  def launch(self, offer):
    """
      Try to launch a MySQL task with the given offer.

      :returns:
        Task ID: Either the task ID of the task just launched or None if this offer is not used.
        Remaining resources: Resources from this offer that are unused by the task. If no task is
            launched, all the resources from the offer are returned.

      :raises IncompatibleRoleError: Raised when the offer has some resource with incompatible role.
    """
    with self._lock:
      if len(self._cluster.active_tasks) == self._cluster.num_nodes:
        # All nodes of this cluster have been launched and none have died.
        return None, offer.resources

      if self._terminating:
        return None, offer.resources

      cpus, mem, disk, ports = self._get_resources(offer.resources)

      task_cpus = self._cluster.cpus
      task_mem = self._cluster.mem
      task_disk = self._cluster.disk

      if cpus < task_cpus or mem < task_mem or disk < task_disk or len(ports) == 0:
        # Offer doesn't fit.
        return None, offer.resources

      log.info("Launcher %s accepted offer %s on Mesos slave %s (%s)" % (
          self.cluster_name, offer.id.value, offer.slave_id.value, offer.hostname))

      task_port = random.choice(list(ports))  # Randomly pick a port in the offer.

      task_info = self._new_task(offer, task_cpus, task_mem, task_disk, task_port)
      self._cluster.tasks[task_info.task_id.value] = MySQLTask(
          self._cluster.name,
          task_info.task_id.value,
          task_info.slave_id.value,
          offer.hostname,
          task_port)
      self._cluster.next_id += 1

      # Checkpoint task data. The task can fail to launch. The problem is solved by the TODO below.
      self._state_provider.dump_cluster_state(self._cluster)

      log.info('Launching task %s on Mesos slave %s (%s)' % (
          task_info.task_id.value, offer.slave_id.value, offer.hostname))

      # Mysos launches at most a single task for each offer. Note that the SchedulerDriver API
      # expects a list of tasks.
      # TODO(jyx): Reconcile after failover because the scheduler can crash before successfully
      #            launching the task. Also run implicit reconciliation periodically.
      self._driver.launchTasks(offer.id, [task_info])

      # Update the offer's resources and return them for other clusters to use.
      remaining = create_resources(
          cpus - task_cpus,
          mem - task_mem,
          disk - task_disk,
          ports - set([task_port]),
          role=self._framework_role)
      return task_info.task_id.value, remaining

  def kill(self, password):
    """
      Kill the cluster.

      NOTE: Cluster killing is asynchronous. Use 'terminated' property to check if all tasks in the
      cluster are killed.
    """
    with self._lock:
      if not self._password_box.match(password, self._cluster.encrypted_password):
        raise self.PermissionError("No permission to kill cluster %s" % self.cluster_name)

      self._terminating = True

      # TODO(jyx): Task killing is unreliable. Reconciliation should retry killing.
      for task_id in self._cluster.tasks:
        log.info("Killing task %s of cluster %s" % (task_id, self.cluster_name))
        self._driver.killTask(mesos_pb2.TaskID(value=task_id))

  @property
  def terminated(self):
    """True if all tasks in the cluster are killed."""
    return self._terminating and len(self._cluster.active_tasks) == 0

  def _get_resources(self, resources):
    """Return a tuple of the resources: cpus, mem, disk, set of ports."""
    cpus, mem, disk, ports = 0.0, Amount(0, Data.MB), Amount(0, Data.MB), set()
    for resource in resources:
      # We do the following check:
      # 1. We only care about the role of the resources we are going to use.
      # 2. For this resource if it is not of the role we want we throw an exception. This implies
      #    that when a slave offers resources that include both the '*' role and the Mysos framework
      #    role we'll decline the entire offer. We expect Mesos slave hosts that run Mysos executors
      #    to dedicate *all* its resources to it as we are not currently optimizing for the use
      #    cases where Mysos tasks run side-by-side with tasks from other frameworks. This also
      #    simplifies the launcher's role filtering logic.
      #    TODO(jyx): Revisit this when the above assumption changes.
      if (resource.name in ('cpus', 'mem', 'disk', 'ports') and
          resource.role != self._framework_role):
        raise self.IncompatibleRoleError("Offered resource %s has role %s, expecting %s" % (
            resource.name, resource.role, self._framework_role))

      if resource.name == 'cpus':
        cpus = resource.scalar.value
      elif resource.name == 'mem':
        # 'Amount' requires an integer while 'value' is double. We convert it bytes to minimize
        # precision loss.
        mem = Amount(int(resource.scalar.value * 1024 * 1024), Data.BYTES)
      elif resource.name == 'disk':
        disk = Amount(int(resource.scalar.value * 1024 * 1024), Data.BYTES)
      elif resource.name == 'ports' and resource.ranges.range:
        for r in resource.ranges.range:
          ports |= set(range(r.begin, r.end + 1))

    return cpus, mem, disk, ports

  def _new_task(self, offer, task_cpus, task_mem, task_disk, task_port):
    """Return a new task with the requested resources."""
    server_id = self._cluster.next_id
    task_id = "mysos-" + self.cluster_name + "-" + str(server_id)

    task = mesos_pb2.TaskInfo()
    task.task_id.value = task_id
    task.slave_id.value = offer.slave_id.value
    task.name = task_id

    task.executor.executor_id.value = task_id  # Use task_id as executor_id.
    task.executor.command.value = self._executor_cmd

    if self._executor_environ:  # Could be 'None' since it's an optional argument.
      executor_environ_ = json.loads(self._executor_environ)
      if executor_environ_:
        for var_ in executor_environ_:
          log.info("Executor will use environment variable: %s" % var_)
          var = task.executor.command.environment.variables.add()
          var.name = var_['name']
          var.value = var_['value']

    uri = task.executor.command.uris.add()
    uri.value = self._executor_uri
    uri.executable = True
    uri.extract = False  # Don't need to decompress pex.

    task.data = json.dumps({
        'framework_user': self._framework_user,
        'host': offer.hostname,
        'port': task_port,
        'cluster': self._cluster.name,
        'cluster_user': self._cluster.user,
        'cluster_password': self._password_box.decrypt(self._cluster.encrypted_password),
        'server_id': server_id,  # Use the integer Task ID as the server ID.
        'zk_url': self._zk_url,
        'admin_keypath': self._admin_keypath,
        'installer_args': self._installer_args,
        'backup_store_args': self._backup_store_args,
        'backup_id': self._cluster.backup_id,
    })

    resources = create_resources(
        task_cpus, task_mem, task_disk, set([task_port]), role=self._framework_role)
    task.resources.extend(resources)

    return task

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

    # Save the epoch so the new elector will use a new epoch after scheduler failover.
    self._state_provider.dump_cluster_state(self._cluster)

    return elector

  def status_update(self, status):
    """
      Handle the status update for a task of this cluster.

      NOTE:
        Duplicate status updates may be handled by either the same scheduler instance or a new
        instance with the restored state.
    """
    with self._lock:
      task_id = status.task_id.value

      if task_id not in self._cluster.tasks:
        log.warn("Ignoring status update for unknown task %s" % task_id)
        return

      task = self._cluster.tasks[task_id]
      previous_state = task.state

      # We don't want to ignore a duplicate update if the previous one was not successfully handled.
      # Therefore, we should not checkpoint the status change until we have finished all operations.
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
          self.cluster_name,
          mesos_pb2.TaskState.Name(previous_state),
          mesos_pb2.TaskState.Name(status.state)))
      task.state = status.state

      if status.state == mesos_pb2.TASK_RUNNING:
        # Register this cluster member.
        endpoint = Endpoint(
            self._cluster.tasks[task_id].hostname, self._cluster.tasks[task_id].port)

        # If the scheduler fails over after ZK is updated but before the state change is
        # checkpointed, it will receive the same status update again and try to publish a duplicate
        # member to ZK. ClusterManager.add_member() is idempotent and doesn't update ZK in this
        # case.
        member_id = self._cluster_manager.add_member(ServiceInstance(endpoint))
        log.info('Added %s (member id=%s) to cluster %s' % (endpoint, member_id, self.cluster_name))
        self._cluster.members[task_id] = member_id

        # Checkpoint the status update here. It's OK if the elector fails to launch later because
        # the new scheduler instance will retry based on the fact that there are running instances
        # of the cluster but no master.
        self._state_provider.dump_cluster_state(self._cluster)

        # If MySQL master is already elected for this cluster don't bother adding it to the elector.
        if self._cluster.master_id:
          log.info(
              "MySQL slave task %s on %s started after a master is already elected for cluster %s" %
              (task_id, endpoint.host, self.cluster_name))
          return

        if not self._elector:
          self._elector = self._new_elector()
          # Add current slaves.
          for t in self._cluster.running_tasks:
            self._elector.add_slave(t.task_id, t.mesos_slave_id)
          self._elector.start()
        else:
          self._elector.add_slave(task_id, status.slave_id.value)
      elif status.state == mesos_pb2.TASK_FINISHED:
        raise self.Error("Task %s is in unexpected state %s with message '%s'" % (
            status.task_id.value,
            mesos_pb2.TaskState.Name(status.state),
            status.message))
      elif is_terminal(status.state):
        if status.state == mesos_pb2.TASK_KILLED:
          log.info("Task %s was successfully killed" % status.task_id.value)
        else:
          log.error("Task %s is now in terminal state %s with message '%s'" % (
              status.task_id.value,
              mesos_pb2.TaskState.Name(status.state),
              status.message))
        del self._cluster.tasks[task_id]

        if task_id in self._cluster.members:
          member_id = self._cluster.members[task_id]
          del self._cluster.members[task_id]

          # If the scheduler fails over after ZK is updated but before its result is persisted, it
          # will receive the same status update and try to remove the non-existent member.
          # Removing a non-existent member is a no-op for ClusterManager.remove_member().
          # Note that if the order is reversed, the scheduler will fail to clean up the orphan ZK
          # entry.
          self._cluster_manager.remove_member(member_id)

          if member_id == self._cluster.master_id:
            self._cluster.master_id = None
            log.info("Master of cluster %s has terminated. Restarting election" % self.cluster_name)

            assert not self._elector, "Election must not be running since there is a current master"
            self._elector = self._new_elector()

            # Add current slaves after removing the terminated task.
            for t in self._cluster.running_tasks:
              self._elector.add_slave(t.task_id, t.mesos_slave_id)
            self._elector.start()
          else:
            # It will be rescheduled next time the launcher is given an offer.
            log.info("Slave %s of cluster %s has terminated" % (task_id, self.cluster_name))
        else:
          assert previous_state != mesos_pb2.TASK_RUNNING, (
              "Task must exist in ClusterManager if it was running")
          log.warn("Slave %s of cluster %s failed to start running" % (task_id, self.cluster_name))

        if self.terminated:
          log.info("Shutting down launcher for cluster %s" % self.cluster_name)
          self._shutdown()
          return

        # Finally, checkpoint the status update.
        self._state_provider.dump_cluster_state(self._cluster)
        log.info("Checkpointed the status update for task %s of cluster %s" % (
            task_id, self.cluster_name))

  def _shutdown(self):
    self._cluster_manager.delete_cluster()
    log.info("Deleted cluster %s from ZooKeeper" % self.cluster_name)
    self._state_provider.remove_cluster_state(self.cluster_name)
    log.info("Removed the state of cluster %s" % self.cluster_name)

    if self._elector:
      self._elector.abort()
      self._elector = None

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

      assert master_task in self._cluster.members, (
          "Elected master must have been added to 'members'")
      master_id = self._cluster.members[master_task]
      slave_host = self._cluster.tasks[master_task].hostname

      log.info('Promoting MySQL task %s on host %s (member ID: %s) as the master for cluster %s' % (
          master_task, slave_host, master_id, self.cluster_name))

      # Persist the elected master before publishing to ZK. If the scheduler fails over before the
      # result is persisted, it re-elects one.
      self._cluster.master_id = master_id
      self._state_provider.dump_cluster_state(self._cluster)

      # Publish the elected master. If the scheduler fails over before the master is published, it
      # republishes it.
      self._cluster_manager.promote_member(master_id)

  def framework_message(self, task_id, slave_id, message):
    with self._lock:
      if self._elector:
        data = json.loads(message)
        self._elector.update_position(int(data["epoch"]), task_id, data["position"])
      else:
        log.info("Received framework message '%s' from task %s (%s) when there is no pending "
            "election" % (message, task_id, slave_id))


# --- Utility methods. ---
def create_resources(cpus, mem, disk, ports, role='*'):
  """Return a list of 'Resource' protobuf for the provided resources."""
  cpus_resources = mesos_pb2.Resource()
  cpus_resources.name = 'cpus'
  cpus_resources.type = mesos_pb2.Value.SCALAR
  cpus_resources.role = role
  cpus_resources.scalar.value = cpus

  mem_resources = mesos_pb2.Resource()
  mem_resources.name = 'mem'
  mem_resources.type = mesos_pb2.Value.SCALAR
  mem_resources.role = role
  mem_resources.scalar.value = mem.as_(Data.MB)

  disk_resources = mesos_pb2.Resource()
  disk_resources.name = 'disk'
  disk_resources.type = mesos_pb2.Value.SCALAR
  disk_resources.role = role
  disk_resources.scalar.value = disk.as_(Data.MB)

  ports_resources = mesos_pb2.Resource()
  ports_resources.name = 'ports'
  ports_resources.type = mesos_pb2.Value.RANGES
  ports_resources.role = role
  for port in ports:
    port_range = ports_resources.ranges.range.add()
    port_range.begin = port
    port_range.end = port

  return [cpus_resources, mem_resources, disk_resources, ports_resources]


def is_terminal(state):
  """Return true if the task reached terminal state."""
  return state in [
    mesos_pb2.TASK_FINISHED,
    mesos_pb2.TASK_KILLED,
    mesos_pb2.TASK_FAILED,
    mesos_pb2.TASK_LOST]
