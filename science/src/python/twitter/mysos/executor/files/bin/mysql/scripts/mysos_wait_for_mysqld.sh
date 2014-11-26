#!/bin/sh
#
# Wait for the mysqld instance to start up and be ready for traffic.
#

path=$1
port=$2
timeout=$3

waited=1
while true
do
  if [ -f $path ]; then
    listen=`netstat -lnt | grep $port | wc -l`
    if [ "x$listen" = "x1" ]; then
      exit 0
    fi
  fi
  sleep 1
  waited=`expr $waited + 1`
  if [ $waited -ge $timeout ]; then
    exit 1
  fi
done
