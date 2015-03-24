#!/bin/sh

set -uex

framework_user=$1
host=$2
port=$3
server_id=$4
data_dir=$5
log_dir=$6
tmp_dir=$7

# Expecting mysqld to be under $mysql_basedir/bin/
mysql_basedir=$(dirname $(dirname $(which mysqld)))

# Need a temp directory under /tmp because the sandbox path is too long for '--socket'. We also need
# this path to be unique on the host.
socket_tmp=`mktemp -d`

# Start the server in read only mode.
mysqld \
  --no-defaults \
  --datadir=$data_dir \
  --user=$framework_user \
  --port=$port \
  --bind-address=0.0.0.0 \
  --socket=$socket_tmp/mysqld.sock \
  --pid-file=$log_dir/mysqld.pid \
  --basedir=$mysql_basedir \
  --tmpdir=$tmp_dir \
  --log_error=$data_dir/error.log \
  --server-id=$server_id \
  --log-bin=master-bin \
  --log-bin-index=master-bin.index \
  --log-slave-updates \
  --relay-log=slave-relay-bin \
  --relay-log-index=slave-relay-bin.index \
  --read-only \
  --report-host=$host \
  --report-port=$port
