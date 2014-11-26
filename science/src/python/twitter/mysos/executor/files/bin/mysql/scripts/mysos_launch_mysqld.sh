#!/bin/sh

set -uex

cluster=$1
host=$2
port=$3
user=$4
serverid=$5
sandbox=$6

instance_home=$sandbox/$cluster/$port

# Expecting mysqld to be under $mysql_basedir/sbin/
mysql_basedir=$(dirname $(dirname $(which mysqld)))

# Need a temp directory under /tmp because the sandbox path is too long for '--socket'.
tmp_dir=`mktemp -d`

# Start the server in read only mode.
mysqld \
  --no-defaults \
  --datadir=$instance_home/data \
  --user=$user \
  --port=$port \
  --bind-address=0.0.0.0 \
  --socket=$tmp_dir/mysqld.sock \
  --pid-file=$instance_home/mysqld.pid \
  --basedir=$mysql_basedir \
  --tmpdir=$instance_home/tmp \
  --log_error=$instance_home/error.log \
  --server-id=$serverid \
  --log-bin=master-bin \
  --log-bin-index=master-bin.index \
  --log-slave-updates \
  --relay-log=slave-relay-bin \
  --relay-log-index=slave-relay-bin.index \
  --read-only \
  --report-host=$host \
  --report-port=$port
