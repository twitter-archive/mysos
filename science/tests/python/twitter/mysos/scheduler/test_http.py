import unittest

from twitter.mysos.scheduler.http import MysosServer
from twitter.mysos.scheduler.scheduler import MysosScheduler

import pytest
from webtest import AppError, TestApp


class FakeScheduler(object):
  def __init__(self):
    self._exception = None

  def set_exception(self, exception):
    self._exception = exception

  def create_cluster(self, name, num_nodes):
    if self._exception:
      raise self._exception


class TestHTTP(unittest.TestCase):
  def setUp(self):
    self._scheduler = FakeScheduler()
    self._app = TestApp(MysosServer(self._scheduler).app)

  def test_create_cluster_exists(self):
    self._scheduler.set_exception(MysosScheduler.ClusterExists())

    with pytest.raises(AppError) as e:
      assert self._app.post('/create/test_cluster', {'num_nodes': 3})
    assert e.value.message.startswith('Bad response: 409')

  def test_create_cluster_value_error(self):
    self._scheduler.set_exception(ValueError())
    with pytest.raises(AppError) as e:
      assert self._app.post('/create/test_cluster', {'num_nodes': 3})
    assert e.value.message.startswith('Bad response: 400')
