#!/bin/sh
#
# Install the MySQL server files before starting 'mysqld'.
#

set -uex

framework_user=$1
data_dir=$2
conf_file=$3

# Expecting mysqld to be under $mysql_basedir/bin/
mysql_basedir=$(dirname $(dirname $(which mysqld)))

# Remove the datadir if any since we are creating a new instance.
rm -rf "${data_dir}"

# Initialize the DB.
mysql_install_db \
  --defaults-file="${conf_file}" \
  --datadir="${data_dir}" \
  --user="${framework_user}" \
  --bind-address=0.0.0.0 \
  --basedir="${mysql_basedir}"
