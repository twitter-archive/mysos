#!/bin/sh
#
# Get the log position of the MySQL slave.
#

set -uxe

slave_host="${1}"
slave_port="${2}"
mysql_command_string="mysql -u root -P ${slave_port} -h ${slave_host} -e \"show slave status\G;\""

# Get the relay log file and position in one step.
relay_master_log_file_and_position=$($mysq_command_string | \
    awk '{if ($1 ~ "Relay_Master_Log_File"){log_file=$2}};
    {if ($1 ~ "Exec_Master_Log_Pos"){log_pos=$2}};
    END {if (length(log_file) != 0 && length(log_pos) != 0)
    {print log_file","log_pos}
    else{exit 1}}')

# The output can be empty if the slave has not set up replication.
printf '%s\n' $relay_master_log_file_and_position
