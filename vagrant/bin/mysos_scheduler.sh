#!/bin/sh

set -uex

TMPDIR=$(mktemp -d)

virtualenv $TMPDIR  # Create venv under /tmp.
$TMPDIR/bin/pip install --find-links /home/vagrant/mysos/deps mesos.native
$TMPDIR/bin/pip install --pre --find-links /home/vagrant/mysos/dist mysos

ZK_HOST=192.168.33.7
API_PORT=55001

# NOTE: In --executor_environ we are pointing MYSOS_DEFAULTS_FILE to an empty MySQL defaults file.
# The file 'my5.6.cnf' is pre-installed by the 'mysql-server-5.6' package on the VM.
$TMPDIR/bin/mysos_scheduler \
    --port=$API_PORT \
    --framework_user=vagrant \
    --mesos_master=zk://$ZK_HOST:2181/mesos/master \
    --executor_uri=/home/vagrant/mysos/dist/mysos-0.1.0-dev0.zip \
    --executor_cmd=/home/vagrant/mysos/vagrant/bin/mysos_executor.sh \
    --zk_url=zk://$ZK_HOST:2181/mysos \
    --admin_keypath=/home/vagrant/mysos/vagrant/etc/admin_keyfile.yml \
    --framework_failover_timeout=1m \
    --framework_role=mysos \
    --framework_authentication_file=/home/vagrant/mysos/vagrant/etc/fw_auth_keyfile.yml \
    --executor_environ='[{"name": "MYSOS_DEFAULTS_FILE", "value": "/etc/mysql/conf.d/my5.6.cnf"}]'
