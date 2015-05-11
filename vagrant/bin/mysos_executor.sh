#!/bin/sh

set -uex

virtualenv venv  # Create vent in the sandbox.

# Using python to run pip and vagrant_mysos_executor because the shebang in venv/bin/pip can
# exceed system limit and cannot be executed directly.

# 'protobuf' is a a dependency of mesos.interface's but we install it separately because otherwise
# 3.0.0-alpha is installed and it breaks the mesos.interface install.
venv/bin/python venv/bin/pip install 'protobuf==2.6.1'
venv/bin/python venv/bin/pip install --find-links /home/vagrant/mysos/deps mesos.native
venv/bin/python venv/bin/pip install --pre --find-links . mysos[executor]

venv/bin/python venv/bin/vagrant_mysos_executor
