#!/bin/sh
#
# Get the log position of the MySQL slave.
#

set -uxe

slave_host=$1
slave_port=$2

# Get the relay log file.
relay_master_log_file=`mysql -u root -P $slave_port -h $slave_host -e "show slave status\G;" | \
  grep Relay_Master_Log_File | awk '{print $2}'`

# Get the relay log position.
exec_master_log_pos=`mysql -u root -P $slave_port -h $slave_host -e "show slave status\G;"  | \
  grep Exec_Master_Log_Pos | awk '{print $2}'`

# The output can be empty if the slave has not set up replication.
echo $relay_master_log_file,$exec_master_log_pos