from __future__ import print_function

import BaseHTTPServer
import json
import time
import urllib
import urllib2

from twitter.common import app, log
from twitter.common.log.options import LogOptions
from twitter.mysos.common.cluster import wait_for_master

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError


app.add_option(
    '--api_host',
    dest='api_host',
    help='Host for the HTTP API server')


app.add_option(
    '--api_port',
    dest='api_port',
    type='int',
    help='Port for the HTTP API server')


app.add_option(
    '--cluster',
    dest='cluster_name',
    help='Name of the MySQL cluster to create')


app.add_option(
    '--cluster_user',
    dest='cluster_user',
    help='MySQL user name the of cluster')


app.add_option(
    '--num_nodes',
    dest='num_nodes',
    type='int',
    help='Number of nodes this cluster should have')


app.add_option(
    '--backup_id',
    dest='backup_id',
    default=None,
    help="The 'backup_id' to restore from")


def main(args, options):
  # This test requires the VM to be properly set up. (e.g. vagrant init ; vagrant up)
  # The logs for mysos and other dependent components are under /var/log/upstart/ in the VM.
  url = 'http://%s:%s/create/%s' % (options.api_host, options.api_port, options.cluster_name)
  values = dict(
      num_nodes=options.num_nodes,
      cluster_user=options.cluster_user,
      backup_id=options.backup_id if options.backup_id else '')

  req = urllib2.Request(url, urllib.urlencode(values))
  try:
    response = urllib2.urlopen(req).read()
  except urllib2.HTTPError as e:
    log.error("POST request failed: %s, %s, %s" %
        (e.code, BaseHTTPServer.BaseHTTPRequestHandler.responses[e.code], e.read()))
    app.quit(1)

  try:
    result = json.loads(response)
    if not isinstance(result, dict):
      raise ValueError()
  except ValueError:
    log.error("Invalid response: %s" % response)
    app.quit(1)

  log.info("Cluster created. Cluster info: %s" % str(result))

  log.info("Waiting for the master for this cluster to be elected...")
  master_endpoint = wait_for_master(result['cluster_url']).service_endpoint

  connection_str = "mysql://%s:%s@%s:%d/" % (
      options.cluster_user, result["cluster_password"], master_endpoint.host, master_endpoint.port)
  log.info("Connecting to the MySQL cluster master: %s" % connection_str)
  engine = create_engine(connection_str)

  for i in range(5):  # Loop for 5 times/seconds to wait for the master to be promoted.
    try:
      # TODO(jyx): Test writing to the master and reading from the slave.
      result = engine.execute("SELECT 1;").scalar()
      assert 1 == int(result), "Expecting result to be 1 but got %s" % result
      break
    except OperationalError:
      if i == 4:
        raise
      log.debug("MySQL master not ready yet. Sleep for 1 second...")
      time.sleep(1)

  log.info("Test successfully completed")

LogOptions.disable_disk_logging()
LogOptions.set_stderr_log_level('google:DEBUG')

app.main()
