import json
import os

from .launcher import MySQLClusterLauncher
from .scheduler import MysosScheduler

import bottle
from mako.template import Template
from twitter.common.http import HttpServer, route, static_file


class MysosServer(HttpServer):
  def __init__(self, scheduler, asset_dir):
    super(MysosServer, self).__init__()
    self._scheduler = scheduler
    self._asset_dir = asset_dir

    self._static_dir = os.path.join(self._asset_dir, 'static')
    self._template_dir = os.path.join(self._asset_dir, 'templates')

    self._clusters_template = Template(filename=os.path.join(self._template_dir, 'clusters.html'))

  @route('/clusters/<clustername>', method=['POST'])
  def create(self, clustername):
    """Create a db cluster."""
    cluster_name = clustername  # For naming consistency.
    num_nodes = bottle.request.forms.get('num_nodes', default=3)
    cluster_user = bottle.request.forms.get('cluster_user', default=None)
    backup_id = bottle.request.forms.get('backup_id', default=None)

    try:
      cluster_zk_url, cluster_password = self._scheduler.create_cluster(
          cluster_name,
          cluster_user,
          num_nodes,
          backup_id=backup_id)
      return json.dumps(dict(cluster_url=cluster_zk_url, cluster_password=cluster_password))
    except MysosScheduler.ClusterExists as e:
      raise bottle.HTTPResponse(e.message, status=409)
    except MysosScheduler.InvalidUser as e:
      raise bottle.HTTPResponse(e.message, status=400)
    except MysosScheduler.ServiceUnavailable as e:
      raise bottle.HTTPResponse(e.message, status=503)
    except ValueError as e:
      raise bottle.HTTPResponse(e.message, status=400)


  @route('/clusters/<clustername>', method=['DELETE'])
  def remove(self, clustername):
    """Remove a db cluster."""
    cluster_name = clustername  # For naming consistency.

    password = bottle.request.forms.get('password', default=None)

    try:
      cluster_zk_url = self._scheduler.delete_cluster(cluster_name, password)
      return json.dumps(dict(cluster_url=cluster_zk_url))
    except MysosScheduler.ClusterNotFound as e:
      raise bottle.HTTPResponse(e.message, status=404)
    except MySQLClusterLauncher.PermissionError as e:
      raise bottle.HTTPResponse(e.message, status=403)

  @route('/', method=['GET'])
  def clusters(self):
    """Landing page, showing the list of managed clusters."""
    if not self._scheduler.connected.is_set():
      return "<h1>Mysos scheduler is still connecting...</h1>"

    return self._clusters_template.render(clusters=self._scheduler.clusters)

  @route('/static/<filepath:path>', method=['GET'])
  def serve_static(self, filepath):
    return static_file(filepath, root=self._static_dir)
