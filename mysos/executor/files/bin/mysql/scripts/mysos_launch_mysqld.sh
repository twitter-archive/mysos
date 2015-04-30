#!/bin/sh

set -uex

framework_user=$1
host=$2
port=$3
server_id=$4
data_dir=$5
log_dir=$6
tmp_dir=$7
conf_file=$8
buffer_pool_size=$9

# Expecting mysqld to be under $mysql_basedir/bin/
mysql_basedir=$(dirname $(dirname $(which mysqld)))

# Need a temp directory under /tmp because the sandbox path is too long for '--socket'. We also need
# this path to be unique on the host.
socket_tmp=`mktemp -d`

# Start the server in read only mode.
mysqld \
  --defaults-file=$conf_file \
  --user=$framework_user \
  --port=$port \
  --server-id=$server_id \
  --socket=$socket_tmp/mysql.sock \
  --pid-file=$log_dir/mysqld.pid \
  --basedir=$mysql_basedir \
  --datadir=$data_dir \
  --tmpdir=$tmp_dir \
  --innodb_data_home_dir=$data_dir \
  --innodb_log_group_home_dir=$log_dir \
  --log-error=$data_dir/mysql-error.log \
  --log-bin=$log_dir/mysql-bin \
  --relay-log=$log_dir/mysql-relay-bin \
  --relay-log-index=$log_dir/mysql-relay-bin.index \
  --innodb_buffer_pool_size=$buffer_pool_size \
  --skip-grant-tables
