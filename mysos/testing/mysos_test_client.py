from __future__ import print_function

import BaseHTTPServer
import json
import os
import tempfile
import time
import urllib
import urllib2

from mysos.common.cluster import wait_for_master, wait_for_termination

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from twitter.common import app, log
from twitter.common.dirutil import safe_mkdir
from twitter.common.log.options import LogOptions


LogOptions.disable_disk_logging()
LogOptions.set_stderr_log_level('google:INFO')


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
    '--password_file',
    dest='password_file',
    default=os.path.join(tempfile.gettempdir(), 'mysos', 'mysos_test_client', 'password_file'),
    help="Path to the file for persisting the cluster password for testing purposes")


def proxy_main():
  @app.command
  @app.command_option(
      '--num_nodes',
      dest='num_nodes',
      type='int',
      help='Number of nodes this cluster should have')
  @app.command_option(
      '--backup_id',
      dest='backup_id',
      default=None,
      help="The 'backup_id' to restore from")
  @app.command_option(
      '--cluster_user',
      dest='cluster_user',
      help='MySQL user name the of cluster')
  def create(args, options):
    validate_common_options(options)

    if not options.num_nodes:
      app.error("--num_nodes is required")

    if not options.cluster_user:
      app.error("--cluster_user is required")

    url = 'http://%s:%s/clusters/%s' % (options.api_host, options.api_port, options.cluster_name)
    values = dict(
        num_nodes=int(options.num_nodes),
        cluster_user=options.cluster_user,
        backup_id=options.backup_id if options.backup_id else '')

    req = urllib2.Request(url, urllib.urlencode(values))
    try:
      response = urllib2.urlopen(req).read()
    except urllib2.HTTPError as e:
      log.error("POST request failed: %s, %s, %s" % (
          e.code, BaseHTTPServer.BaseHTTPRequestHandler.responses[e.code], e.read()))
      app.quit(1)

    try:
      result = json.loads(response)
      if not isinstance(result, dict):
        raise ValueError()
    except ValueError:
      log.error("Invalid response: %s" % response)
      app.quit(1)

    log.info("Cluster created. Cluster info: %s" % str(result))
    with open(options.password_file, 'w') as f:
      f.write(result["cluster_password"])

    log.info("Waiting for the master for this cluster to be elected...")
    master_endpoint = wait_for_master(result['cluster_url']).service_endpoint

    connection_str = "mysql://%s:%s@%s:%d/" % (
        options.cluster_user,
        result["cluster_password"],
        master_endpoint.host,
        master_endpoint.port)
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

    log.info("Cluster successfully started")

  @app.command
  def delete(args, options):
    validate_common_options(options)

    with open(options.password_file, 'r') as f:
      password = f.read().strip()
      if not password:
        app.error("Empty password file")

    url = 'http://%s:%s/clusters/%s' % (options.api_host, options.api_port, options.cluster_name)
    values = dict(password=password)

    req = urllib2.Request(url, urllib.urlencode(values))
    req.get_method = lambda: 'DELETE'

    try:
      response = urllib2.urlopen(req).read()
    except urllib2.HTTPError as e:
      log.error("DELETE request failed: %s, %s, %s" % (
          e.code, BaseHTTPServer.BaseHTTPRequestHandler.responses[e.code], e.read()))
      app.quit(1)

    try:
      result = json.loads(response)
      if not isinstance(result, dict):
        raise ValueError()
    except ValueError:
      log.error("Invalid response: %s" % response)
      app.quit(1)

    log.info("Cluster deletion result: %s" % result)

    log.info("Waiting for the cluster to terminate...")
    wait_for_termination(result['cluster_url'])

    log.info("Cluster terminated/deleted")

  def validate_common_options(options):
    if not options.api_host:
      app.error("--api_host is required")

    if not options.api_port:
      app.error("--api_port is required")

    if not options.cluster_name:
      app.error("--cluster is required")

    if not options.password_file:
      app.error("--password_file is required")
    log.info("Using --password_file=%s" % options.password_file)
    safe_mkdir(os.path.dirname(options.password_file))

  def main(args, options):
    app.help()

  app.main()
