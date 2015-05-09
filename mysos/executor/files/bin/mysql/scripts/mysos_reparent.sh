#!/bin/sh
#
# Reparent the slave to a new master.
#

set -uxe

master_host=$1
master_port=$2
slave_host=$3
slave_port=$4
admin_user=$5
admin_passwd=$6

# Stop the replication (if any).
mysql -u root -P $slave_port -h $slave_host -e "STOP SLAVE;"

relay_master_log_file_and_position=$( \
  mysql -u root -P "${slave_port}" -h "${slave_host}" -e "show slave status\G;" | \
  awk '
  {
    if ($1 ~ "Relay_Master_Log_File"){
      log_file=$2
    }
  }
  {
    if ($1 ~ "Exec_Master_Log_Pos"){
      log_pos=$2
    }
  }
  END{
    if (length(log_file) != 0 && length(log_pos) != 0){
      print log_file","log_pos
    }
  }')

  relay_master_log_file=$(echo $relay_master_log_file_and_position | \
    awk -F, '{print $1}')
  exec_master_log_pos=$(echo $relay_master_log_file_and_position | \
    awk -F, '{print $2}')

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
mysql -u root -P "${slave_port}" -h "${slave_host}" -e "START SLAVE;"
