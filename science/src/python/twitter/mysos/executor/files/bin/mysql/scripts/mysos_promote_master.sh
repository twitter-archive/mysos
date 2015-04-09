#!/bin/sh
#
# Promote the MySQL slave to be a master.
#

set -uex

host=$1
port=$2
user=$3
password=$4
admin_user=$5
admin_passwd=$6

# Stop and reset the slave.
mysql -u root -P $port -h $host -e "STOP SLAVE; RESET SLAVE ALL;

# Put the master in read-write mode.
SET GLOBAL read_only = 0; UNLOCK TABLES;

# This reloads the grant tables so the following account-management statements can work.
FLUSH PRIVILEGES;

# Grant all permissions to the admin user (and create it if not exists).
GRANT ALL ON *.* to '$admin_user'@'%' IDENTIFIED BY '$admin_passwd' WITH GRANT OPTION;

# Grant all permissions to the user (and create it if not exists).
GRANT ALL ON *.* to '$user'@'%' IDENTIFIED BY '$password' WITH GRANT OPTION;

# Drop anonymous user.
DELETE FROM mysql.user WHERE user=''; FLUSH PRIVILEGES;"
