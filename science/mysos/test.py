#!/usr/bin/env python

from __future__ import print_function

import BaseHTTPServer
import os
import time
import urllib
import urllib2
import subprocess
import sys


# This test requires the VM to be properly set up. (e.g. vagrant init ; vagrant up)
# The logs for mysos and other dependent components are under /var/log/upstart/ in the VM.
# TODO(jyx): Implement more rigorous integration tests using the VM.
num_nodes = 3
host = "192.168.33.7"
port = 55001

url = 'http://%s:%s/create/test_cluster' % (host, port)
values = {'num_nodes': num_nodes}

req = urllib2.Request(url, urllib.urlencode(values))
try:
  response = urllib2.urlopen(req)
  result = response.read()
except urllib2.HTTPError as e:
  print("POST request failed: %s, %s, %s" %
        (e.code, BaseHTTPServer.BaseHTTPRequestHandler.responses[e.code], e.read()))
  sys.exit(1)

print("Sleeping for 60 secs to wait for tasks to be launched...")
time.sleep(60)

# The scheduler currently just launches a 'tail -f /dev/null' process for each task so this verifies
# that there are indeed 3 such processes.
result = subprocess.check_output(
    "vagrant ssh -c 'pgrep tail | wc -l' -- -q",
    shell=True,
    cwd=os.path.dirname(os.path.realpath(__file__)))
assert int(result) == num_nodes, result
