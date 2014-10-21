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
    num_nodes = bottle.request.forms.get('num_nodes', default=3)

    try:
      self._scheduler.create_cluster(clustername, num_nodes)
    except MysosScheduler.ClusterExists as e:
      raise bottle.HTTPResponse(e.message, status=409)
    except ValueError as e:
      raise bottle.HTTPResponse(e.message, status=400)
