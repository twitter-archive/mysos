import functools
import posixpath
import threading

from twitter.common.zookeeper.serverset.endpoint import ServiceInstance

from kazoo.exceptions import NoNodeError
from kazoo.recipe.watchers import ChildrenWatch


class Cluster(object):
  """
    A class that represents all members of the MySQL cluster.

    A newly added cluster member becomes a read-only slave until it's promoted to a master.
    Only the master can write.

    The members of the cluster are maintained in two ZooKeeper groups: a slaves group and a master
    group under the same 'root'. Slaves have unique member IDs backed by ZooKeeper's sequential
    ZNodes. When a slave is promoted to a master, it is moved (its ID preserved) from the slave
    group to the master group.

    There is at most one member in the master group.
  """
  SLAVES_GROUP = 'slaves'
  MASTER_GROUP = 'master'
  MEMBER_PREFIX = "member_"  # Use the prefix so the path conforms to the ServerSet convention.

  def __init__(self, root):
    self.members = {}  # {ID : ServiceInstance} mappings for members of the cluster.
    self.master = None  # The master's member ID.
    self.slaves_group = posixpath.join(root, self.SLAVES_GROUP)
    self.master_group = posixpath.join(root, self.MASTER_GROUP)


# TODO(jyx): Handle errors e.g. sessions expirations and recoverable failures.
class ClusterManager(object):
  """
    Kazoo wrapper used by the scheduler to inform executors about cluster change.
    NOTE: ClusterManager is thread safe, i.e., it can be accessed from multiple threads at once.
  """

  def __init__(self, client, root):
    """
      :param client: Kazoo client.
      :param root: The root path for this cluster on ZooKeeper.
    """
    self._client = client
    self._cluster = Cluster(root)
    self._lock = threading.Lock()
    self._populate()

  def _read_child(self, group, member_id):
    try:
      content, _ = self._client.get(posixpath.join(group, member_id))
      return ServiceInstance.unpack(content)
    except NoNodeError:
      return None

  def _populate(self):
    self._client.ensure_path(self._cluster.slaves_group)
    self._client.ensure_path(self._cluster.master_group)

    # Populate slaves.
    for child in self._client.get_children(self._cluster.slaves_group):
      child_content = self._read_child(self._cluster.slaves_group, child)
      if child_content:
        self._cluster.members[child] = child_content

    # Populate the master.
    master_group = self._client.get_children(self._cluster.master_group)
    assert len(master_group) <= 1
    if len(master_group) == 1:
      child = master_group[0]
      child_content = self._read_child(self._cluster.master_group, child)
      if child_content:
        self._cluster.members[child] = child_content
        self._cluster.master = child

  def add_member(self, service_instance):
    """
      Add the member to ZooKeeper and return the ID of the added member.
      NOTE: New members are slaves until being promoted.
    """
    content = ServiceInstance.pack(service_instance)
    znode_path = self._client.create(
        posixpath.join(self._cluster.slaves_group, self._cluster.MEMBER_PREFIX),
        content,
        sequence=True)
    _, member_id = posixpath.split(znode_path)
    with self._lock:
      self._cluster.members[member_id] = content
      return member_id

  def remove_member(self, member_id):
    with self._lock:
      if member_id not in self._cluster.members:
        raise ValueError("Invalid member_id")

      self._cluster.members.pop(member_id, None)

      if member_id == self._cluster.master:
        self._cluster.master = None
        self._client.delete(posixpath.join(self._cluster.master_group, member_id))
      else:
        self._client.delete(posixpath.join(self._cluster.slaves_group, member_id))

  def promote_member(self, member_id):
    with self._lock:
      if member_id not in self._cluster.members:
        raise ValueError("Invalid member_id")

      # Do nothing if the member is already the master.
      if self._cluster.master and self._cluster.master == member_id:
        return

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
      return self._cluster.master


# TODO(wickman): Implement kazoo connection acquiescence.
class ClusterListener(object):
  """Kazoo wrapper used by the executor to listen to cluster change."""

  def __init__(self,
               client,
               root,
               self_instance,
               promotion_callback=None,
               demotion_callback=None,
               master_callback=None):
    """
      :param client: Kazoo client.
      :param root: The root path for this cluster on ZooKeeper.
      :param self_instance: The local ServiceInstance associated with this listener.
      :param promotion_callback: Invoked when 'self_instance' is promoted.
      :param demotion_callback: Invoked when 'self_instance' is demoted.
      :param master_callback: Invoked when there is a master change otherwise.
      NOTE: Callbacks are executed synchronously in Kazoo's completion thread to ensure the delivery
      order of events. Blocking the callback method means no future callbacks will be invoked.
    """
    self._client = client
    self._cluster = Cluster(root)
    self._self_content = ServiceInstance.pack(self_instance)
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
    i_was_master = self._master_content == self._self_content
    self._master, self._master_content = master, master_content
    i_am_master = self._master_content == self._self_content

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
