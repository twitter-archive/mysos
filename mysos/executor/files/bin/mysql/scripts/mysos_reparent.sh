#!/bin/sh
#
# Reparent the slave to a new master.
#

set -ue  # No -x due to passwords in the commands.

master_host=$1
master_port=$2
slave_host=$3
slave_port=$4
admin_user=$5
admin_passwd=$6

# Stop the replication (if any).
mysql -u root -P $slave_port -h $slave_host -e "STOP SLAVE;"

# Get the relay log file.
relay_master_log_file=`mysql -u root -P $slave_port -h $slave_host -e "show slave status\G;" | \
  grep Relay_Master_Log_File | awk '{print $2}'`

# Get the relay log position.
exec_master_log_pos=`mysql -u root -P $slave_port -h $slave_host -e "show slave status\G;" | \
  grep Exec_Master_Log_Pos | awk '{print $2}'`

# TODO(vinod): Make sure the slave processed the relay log.

# Remove the relay logs.
mysql -u root -P $slave_port -h $slave_host -e "RESET SLAVE;"

# Point to a master.
if [ "x$relay_master_log_file" != "x" ]; then
  mysql -u root -P $slave_port -h $slave_host -e "CHANGE MASTER TO
    MASTER_HOST='$master_host',
    MASTER_PORT=$master_port,
    MASTER_USER='$admin_user',
    MASTER_PASSWORD='$admin_passwd',
    MASTER_LOG_FILE='$relay_master_log_file',
    MASTER_LOG_POS=$exec_master_log_pos;"
else
  mysql -u root -P $slave_port -h $slave_host -e "CHANGE MASTER TO
    MASTER_HOST='$master_host',
    MASTER_PORT=$master_port,
    MASTER_USER='$admin_user',
    MASTER_PASSWORD='$admin_passwd';"
fi

# Start replication.
mysql -u root -P $slave_port -h $slave_host -e "START SLAVE;"
