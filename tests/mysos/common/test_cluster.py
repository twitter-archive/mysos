import Queue
import threading
import unittest

from twitter.common.zookeeper.serverset.endpoint import Endpoint, ServiceInstance
from twitter.mysos.common.cluster import ClusterListener, ClusterManager

from kazoo.handlers.threading import SequentialThreadingHandler
import pytest
from zake.fake_client import FakeClient
from zake.fake_storage import FakeStorage


class CallbackHandler(object):
  """Utility for testing callbacks."""

  def __init__(self):
    self.promoted = threading.Event()
    self.demoted = threading.Event()
    self.detected = Queue.Queue()
    self.terminated = threading.Event()

  def promotion_callback(self):
    self.promoted.set()

  def demotion_callback(self):
    self.demoted.set()

  def master_callback(self, master):
    self.detected.put(master)

  def termination_callback(self):
    self.terminated.set()


class TestCluster(unittest.TestCase):
  def setUp(self):
    self.storage = FakeStorage(SequentialThreadingHandler())
    self.client = FakeClient(storage=self.storage)
    self.client.start()

  def tearDown(self):
    self.client.stop()

  def test_add_member(self):
    manager = ClusterManager(self.client, "/home/my_cluster")

    instance1 = ServiceInstance(Endpoint("host1", 10000))
    member1 = manager.add_member(instance1)
    assert member1 == manager.add_member(instance1)  # Second insertion is ignored.

    instance2 = ServiceInstance(Endpoint("host2", 10000))
    manager.add_member(instance2)

    assert len(manager._cluster.members) == 2

    assert (self.storage.paths["/home/my_cluster/slaves/member_0000000000"]["data"] ==
            ServiceInstance.pack(instance1))
    assert (self.storage.paths["/home/my_cluster/slaves/member_0000000001"]["data"] ==
            ServiceInstance.pack(instance2))

  def test_promote_member(self):
    manager = ClusterManager(self.client, "/home/my_cluster")
    instance = ServiceInstance(Endpoint("host", 10000))
    member = manager.add_member(instance)

    assert manager.promote_member(member)
    assert not manager.promote_member(member)  # The 2nd promotion is a no-op.

    assert (self.storage.paths["/home/my_cluster/master/member_0000000000"]["data"] ==
            ServiceInstance.pack(instance))

  def test_remove_member(self):
    manager = ClusterManager(self.client, "/home/my_cluster")
    instance = ServiceInstance(Endpoint("host", 10000))
    member = manager.add_member(instance)

    assert manager.remove_member(member)
    assert not manager.remove_member(member)  # The second deletion is ignored.

    assert "/home/my_cluster/master/member_0000000000" not in self.storage.paths

  def test_callbacks(self):
    manager = ClusterManager(self.client, "/home/my_cluster")

    # Set up 2 listeners.
    instance1 = ServiceInstance(Endpoint("host1", 10000))
    handler1 = CallbackHandler()
    listener1 = ClusterListener(
        self.client,
        "/home/my_cluster",
        instance1,
        handler1.promotion_callback,
        handler1.demotion_callback,
        handler1.master_callback,
        handler1.termination_callback)
    listener1.start()
    member1 = manager.add_member(instance1)

    instance2 = ServiceInstance(Endpoint("host2", 10000))
    handler2 = CallbackHandler()
    listener2 = ClusterListener(
        self.client,
        "/home/my_cluster",
        instance2,
        handler2.promotion_callback,
        handler2.demotion_callback,
        handler2.master_callback)
    listener2.start()
    member2 = manager.add_member(instance2)

    # Test promotion.
    manager.promote_member(member1)

    assert handler1.promoted.wait(1)
    assert handler2.detected.get(True, 1) == instance1

    assert (self.storage.paths["/home/my_cluster/master/member_0000000000"]["data"] ==
            ServiceInstance.pack(instance1))
    assert (self.storage.paths["/home/my_cluster/slaves/member_0000000001"]["data"] ==
            ServiceInstance.pack(instance2))

    manager.promote_member(member2)

    assert handler1.demoted.wait(1)
    assert handler2.promoted.wait(1)

    assert (self.storage.paths["/home/my_cluster/master/member_0000000001"]["data"] ==
            ServiceInstance.pack(instance2))
    assert "/home/my_cluster/master/member_0000000000" not in self.storage.paths

    manager.remove_member(member2)
    assert handler2.demoted.wait(1)

    # Test removing cluster.
    manager.remove_member(member1)
    manager.delete_cluster()
    assert handler1.terminated.wait(1)

  def test_invalid_arguments(self):
    client = FakeClient()
    client.start()
    manager = ClusterManager(client, "/home/my_cluster")

    with pytest.raises(ValueError) as e:
      manager.promote_member("123")
    assert e.value.message == 'Invalid member_id: 123'

  def test_invalid_znode(self):
    instance1 = ServiceInstance(Endpoint("host1", 10000))
    handler1 = CallbackHandler()
    listener1 = ClusterListener(
        self.client,
        "/home/my_cluster",
        instance1,
        handler1.promotion_callback,
        handler1.demotion_callback,
        handler1.master_callback)
    listener1.start()

    self.client.ensure_path("/home/my_cluster/master")
    self.client.create("/home/my_cluster/master/member_", "Invalid Data", sequence=True)

    # Invalid ZNode data translates into a 'None' return.
    assert handler1.detected.get(True, 1) is None

  def test_existing_zk(self):
    """
      ClusterManager needs to be able to recover from an existing ZK group for scheduler failover.
    """
    manager = ClusterManager(self.client, "/home/my_cluster")

    instance1 = ServiceInstance(Endpoint("host1", 10000))
    member1 = manager.add_member(instance1)
    instance2 = ServiceInstance(Endpoint("host2", 10000))
    member2 = manager.add_member(instance2)

    assert (self.storage.paths["/home/my_cluster/slaves/member_0000000000"]["data"] ==
            ServiceInstance.pack(instance1))
    assert (self.storage.paths["/home/my_cluster/slaves/member_0000000001"]["data"] ==
            ServiceInstance.pack(instance2))

    manager.promote_member(member1)

    # Test the new ClusterManager.
    manager2 = ClusterManager(self.client, "/home/my_cluster")
    assert len(manager2._cluster.members) == 2
    assert member1 in manager2._cluster.members
    assert member2 in manager2._cluster.members
    assert manager2._cluster.members[member1] == ServiceInstance.pack(instance1)

  def test_remove_cluster(self):
    manager = ClusterManager(self.client, "/home/my_cluster")

    instance1 = ServiceInstance(Endpoint("host1", 10000))
    member1 = manager.add_member(instance1)
    instance2 = ServiceInstance(Endpoint("host2", 10000))
    member2 = manager.add_member(instance2)

    manager.promote_member(member1)

    with pytest.raises(ClusterManager.Error) as e:
      manager.delete_cluster()

    manager.remove_member(member1)
    manager.remove_member(member2)
    manager.delete_cluster()

    assert "/home/my_cluster" not in self.storage.paths
