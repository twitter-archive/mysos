import json

from twitter.common.http import HttpServer, route

from .scheduler import MysosScheduler

import bottle


class MysosServer(HttpServer):
  def __init__(self, scheduler):
    super(MysosServer, self).__init__()
    self._scheduler = scheduler

  @route('/create/<clustername>', method=['POST'])
  def create(self, clustername):
    """Create a db cluster."""
    cluster_name = clustername  # For naming consistency.
    num_nodes = bottle.request.forms.get('num_nodes', default=3)
    cluster_user = bottle.request.forms.get('cluster_user', default=None)

    try:
      cluster_zk_url, cluster_password = self._scheduler.create_cluster(
          cluster_name,
          cluster_user,
          num_nodes)
      return json.dumps(dict(cluster_url=cluster_zk_url, cluster_password=cluster_password))
    except MysosScheduler.ClusterExists as e:
      raise bottle.HTTPResponse(e.message, status=409)
    except MysosScheduler.InvalidUser as e:
      raise bottle.HTTPResponse(e.message, status=400)
    except MysosScheduler.ServiceUnavailable as e:
      raise bottle.HTTPResponse(e.message, status=503)
    except ValueError as e:
      raise bottle.HTTPResponse(e.message, status=400)

  @route('/', method=['GET'])
  def home(self):
    """Landing page."""
    # TODO(jyx): Create a system status page.
    if not self._scheduler.connected.is_set():
      return "<h1>Mysos scheduler is still connecting...</h1>"
    else:
      return "<h1>Mysos scheduler is available</h1>"
