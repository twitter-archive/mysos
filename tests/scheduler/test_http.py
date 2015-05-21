import json
import shutil
import tempfile
import unittest

from mysos.common import pkgutil
from mysos.scheduler.http import MysosServer
from mysos.scheduler.scheduler import MysosScheduler

import pytest
from webtest import AppError, TestApp
from twitter.common.metrics import MetricSampler, RootMetrics


MYSOS_MODULE = 'mysos.scheduler'
ASSET_RELPATH = 'assets'


class FakeScheduler(object):
  def __init__(self):
    self._exception = None
    self._response = None

  def set_exception(self, exception):
    self._exception = exception

  def set_response(self, response):
    self._response = response

  def create_cluster(
      self,
      cluster_name,
      cluster_user,
      num_nodes,
      size,
      backup_id=None,
      cluster_password=None):
    if self._exception:
      raise self._exception
    return self._response


class TestHTTP(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    cls.web_assets_dir = tempfile.mkdtemp()
    pkgutil.unpack_assets(cls.web_assets_dir, MYSOS_MODULE, ASSET_RELPATH)

  @classmethod
  def tearDownClass(cls):
    shutil.rmtree(cls.web_assets_dir)

  def setUp(self):
    self._scheduler = FakeScheduler()
    self._app = TestApp(
        MysosServer(self._scheduler, self.web_assets_dir, MetricSampler(RootMetrics())).app)

  def test_create_cluster_successful(self):
    response = ('test_cluster_url', 'passwordfortestcluster')
    self._scheduler.set_response(response)
    body = self._app.post(
        '/clusters/test_cluster', {'num_nodes': 3, 'cluster_user': 'mysos'}).normal_body
    assert json.loads(body) == dict(cluster_url=response[0], cluster_password=response[1])

  def test_create_cluster_exists(self):
    self._scheduler.set_exception(MysosScheduler.ClusterExists())

    with pytest.raises(AppError) as e:
      assert self._app.post('/clusters/test_cluster', {'num_nodes': 3, 'cluster_user': 'mysos'})
    assert e.value.message.startswith('Bad response: 409')

  def test_create_cluster_value_error(self):
    self._scheduler.set_exception(ValueError())
    with pytest.raises(AppError) as e:
      self._app.post('/clusters/test_cluster', {'num_nodes': 3, 'cluster_user': 'mysos'})
    assert e.value.message.startswith('Bad response: 400')

  def test_create_cluster_invalid_user(self):
    self._scheduler.set_exception(MysosScheduler.InvalidUser())
    with pytest.raises(AppError) as e:
      self._app.post('/clusters/test_cluster', {'num_nodes': 3, 'cluster_user': 'mysos'})
    assert e.value.message.startswith('Bad response: 400')
