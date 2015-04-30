#!/bin/sh

set -uex

virtualenv venv  # Create vent in the sandbox.

# Using python to run pip and vagrant_mysos_executor because the shebang in venv/bin/pip can
# exceed system limit and cannot be executed directly.
venv/bin/python venv/bin/pip install --find-links /home/vagrant/mysos/deps mesos.native
venv/bin/python venv/bin/pip install --pre --find-links . mysos

venv/bin/python venv/bin/vagrant_mysos_executor
