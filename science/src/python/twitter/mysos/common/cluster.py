import Queue
import functools
import posixpath
import sys
import threading

from twitter.common import log
from twitter.common.zookeeper.serverset.endpoint import ServiceInstance
from twitter.mysos.common import zookeeper

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.recipe.watchers import ChildrenWatch


def get_cluster_path(zk_root, cluster_name):
  """
    :param zk_root: the root path for the mysos scheduler.
    :param cluster_name: Name of the the cluster.
    :return: The path for the cluster.
  """
  return posixpath.join(zk_root, cluster_name)


class Cluster(object):
  """
    A class that represents all members of the MySQL cluster.

    A newly added cluster member becomes a read-only slave until it's promoted to a master.
    Only the master can write.

    The members of the cluster are maintained in two ZooKeeper groups: a slaves group and a master
    group under the same 'directory'. Slaves have unique member IDs backed by ZooKeeper's sequential
    ZNodes. When a slave is promoted to a master, it is moved (its ID preserved) from the slave
    group to the master group.

    There is at most one member in the master group.
  """
  SLAVES_GROUP = 'slaves'
  MASTER_GROUP = 'master'
  MEMBER_PREFIX = "member_"  # Use the prefix so the path conforms to the ServerSet convention.

  def __init__(self, cluster_path):
    self.members = {}  # {ID : (serialized) ServiceInstance} mappings for members of the cluster.
    self.master = None  # The master's member ID.
    self.slaves_group = posixpath.join(cluster_path, self.SLAVES_GROUP)
    self.master_group = posixpath.join(cluster_path, self.MASTER_GROUP)


# TODO(jyx): Handle errors e.g. sessions expirations and recoverable failures.
class ClusterManager(object):
  """
    Kazoo wrapper used by the scheduler to inform executors about cluster change.
    NOTE: ClusterManager is thread safe, i.e., it can be accessed from multiple threads at once.
  """

  def __init__(self, client, cluster_path):
    """
      :param client: Kazoo client.
      :param cluster_path: The path for this cluster on ZooKeeper.
    """
    self._client = client
    self._cluster = Cluster(cluster_path)
    self._lock = threading.Lock()
    self._populate()

  def _read_child_content(self, group, member_id):
    try:
      return self._client.get(posixpath.join(group, member_id))[0]
    except NoNodeError:
      return None

  def _populate(self):
    self._client.ensure_path(self._cluster.slaves_group)
    self._client.ensure_path(self._cluster.master_group)

    # Populate slaves.
    for child in self._client.get_children(self._cluster.slaves_group):
      child_content = self._read_child_content(self._cluster.slaves_group, child)
      if child_content:
        self._cluster.members[child] = child_content

    # Populate the master.
    master_group = self._client.get_children(self._cluster.master_group)
    assert len(master_group) <= 1
    if len(master_group) == 1:
      child = master_group[0]
      child_content = self._read_child_content(self._cluster.master_group, child)
      if child_content:
        self._cluster.members[child] = child_content
        self._cluster.master = child

  def add_member(self, service_instance):
    """
      Add the member to the ZooKeeper group.
      NOTE:
        - New members are slaves until being promoted.
        - A new member is not added if the specified service_instance already exists in the group.
      :return: The member ID for the ServiceInstance generated by ZooKeeper.
    """
    if not isinstance(service_instance, ServiceInstance):
      raise TypeError("'service_instance' should be a ServiceInstance")

    content = ServiceInstance.pack(service_instance)

    for k, v in self._cluster.members.items():
      if content == v:
        log.info("%s not added because it already exists in the group" % service_instance)
        return k

    znode_path = self._client.create(
        posixpath.join(self._cluster.slaves_group, self._cluster.MEMBER_PREFIX),
        content,
        sequence=True)
    _, member_id = posixpath.split(znode_path)
    with self._lock:
      self._cluster.members[member_id] = content
      return member_id

  def remove_member(self, member_id):
    """
      Remove the member if it is in the group.

      :return: True if the member is deleted. False if the member cannot be found.
    """
    with self._lock:
      if member_id not in self._cluster.members:
        log.info("Member %s is not in the ZK group" % member_id)
        return False

      self._cluster.members.pop(member_id, None)

      if member_id == self._cluster.master:
        self._cluster.master = None
        self._client.delete(posixpath.join(self._cluster.master_group, member_id))
      else:
        self._client.delete(posixpath.join(self._cluster.slaves_group, member_id))

      return True

  def promote_member(self, member_id):
    """
      Promote the member with the given ID to be the master of the cluster if it's not already the
      master.

      :return: True if the member is promoted. False if the member is already the master.
    """
    with self._lock:
      if member_id not in self._cluster.members:
        raise ValueError("Invalid member_id: %s" % member_id)

      # Do nothing if the member is already the master.
      if self._cluster.master and self._cluster.master == member_id:
        log.info("Not promoting %s because is already the master" % member_id)
        return False

      tx = self._client.transaction()
      if self._cluster.master:
        tx.delete(posixpath.join(self._cluster.master_group, self._cluster.master))
        self._cluster.members.pop(self._cluster.master)

      # "Move" the ZNode, i.e., create a ZNode of the same ID in the master group.
      tx.delete(posixpath.join(self._cluster.slaves_group, member_id))
      tx.create(
          posixpath.join(self._cluster.master_group, member_id),
          self._cluster.members[member_id])

      tx.commit()

      self._cluster.master = member_id

      return True


# TODO(wickman): Implement kazoo connection acquiescence.
class ClusterListener(object):
  """Kazoo wrapper used by the executor to listen to cluster change."""

  def __init__(self,
               client,
               cluster_path,
               self_instance=None,
               promotion_callback=None,
               demotion_callback=None,
               master_callback=None):
    """
      :param client: Kazoo client.
      :param cluster_path: The path for this cluster on ZooKeeper.
      :param self_instance: The local ServiceInstance associated with this listener.
      :param promotion_callback: Invoked when 'self_instance' is promoted.
      :param demotion_callback: Invoked when 'self_instance' is demoted.
      :param master_callback: Invoked when there is a master change otherwise.
      NOTE: Callbacks are executed synchronously in Kazoo's completion thread to ensure the delivery
            order of events. Blocking the callback method means no future callbacks will be invoked.
    """
    self._client = client
    self._cluster = Cluster(cluster_path)
    self._self_content = ServiceInstance.pack(self_instance) if self_instance else None
    self._master = None
    self._master_content = None
    self._promotion_callback = promotion_callback or (lambda: True)
    self._demotion_callback = demotion_callback or (lambda: True)
    self._master_callback = master_callback or (lambda x: True)

  def start(self):
    """Start the listener to watch the master group."""
    # ChildrenWatch only works with an existing path.
    self._client.ensure_path(self._cluster.master_group)
    ChildrenWatch(self._client, self._cluster.master_group, func=self._child_callback)

  def _swap(self, master, master_content):
    i_was_master = self._self_content and self._master_content == self._self_content
    self._master, self._master_content = master, master_content
    i_am_master = self._self_content and self._master_content == self._self_content

    # Invoke callbacks accordingly.
    # NOTE: No callbacks are invoked if there is currently no master and 'self_instance' wasn't the
    # master.
    if i_was_master and not i_am_master:
      self._demotion_callback()
    elif not i_was_master and i_am_master:
      self._promotion_callback()
    elif not i_was_master and not i_am_master and master:
      assert master_content
      self._master_callback(ServiceInstance.unpack(master_content))

  def _data_callback(self, master_id, master_completion):
    try:
      master_content, _ = master_completion.get()
    except NoNodeError:
      # ZNode could be gone after we detected it but before we read it.
      master_id, master_content = None, None
    self._swap(master_id, master_content)

  def _child_callback(self, masters):
    assert len(masters) <= 1, "There should be at most one master"

    if len(masters) == 1 and self._master != masters[0]:
      self._client.get_async(posixpath.join(self._cluster.master_group, masters[0])).rawlink(
          functools.partial(self._data_callback, masters[0]))
    elif len(masters) == 0:
      self._swap(None, None)


def resolve_master(cluster_url, master_callback, zk_client=None):
  """
    Resolve the MySQL cluster master's endpoint from the given URL for this cluster.
    :param cluster_url: The ZooKeeper URL for this cluster.
    :param master_callback: A callback method with one argument: the ServiceInstance for the elected
                            master.
    :param zk_client: Use a custom ZK client instead of Kazoo if specified.
  """
  try:
    _, zk_servers, cluster_path = zookeeper.parse(cluster_url)
  except Exception as e:
    raise ValueError("Invalid cluster_url: %s" % e.message)

  if not zk_client:
    zk_client = KazooClient(zk_servers)
    zk_client.start()

  listener = ClusterListener(zk_client, cluster_path, None, master_callback=master_callback)
  listener.start()


def wait_for_master(cluster_url, zk_client=None):
  """
    Convenience function to wait for the master to be elected and return the master.
    :param cluster_url: The ZooKeeper URL for this cluster.
    :param zk_client: Use a custom ZK client instead of Kazoo if specified.
    :return: The ServiceInstance for the elected master.
  """
  master = Queue.Queue()
  resolve_master(cluster_url, lambda x: master.put(x), zk_client)
  # Block forever but using sys.maxint makes the wait interruptable by Ctrl-C. See
  # http://bugs.python.org/issue1360.
  return master.get(True, sys.maxint)
