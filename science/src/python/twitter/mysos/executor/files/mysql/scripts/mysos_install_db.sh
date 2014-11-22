#!/bin/sh
#
# Install the MySQL server files before starting 'mysqld'.
#

set -uex

cluster_name=$1
port=$2
user=$3
sandbox=$4

# Home for this particular mysqld instance.
instance_home=$sandbox/$cluster_name/$port

# Expecting mysqld to be under $mysql_basedir/sbin/
mysql_basedir=$(dirname $(dirname $(which mysqld)))

# Remove the home directory.
rm -rf $instance_home

# Create temp dir.
mkdir -p $instance_home/tmp

# Initialize the DB.
mysql_install_db \
  --datadir=$instance_home/data \
  --user=$user \
  --bind-address=0.0.0.0 \
  --basedir=$mysql_basedir \
  --no-defaults