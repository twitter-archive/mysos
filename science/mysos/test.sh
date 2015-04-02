#!/bin/sh

host="192.168.33.7"
port=55001
cluster_name="test_cluster$((RANDOM % 1000))"
num_nodes=2
cluster_user="mysos"

mysos_dir="$(cd "$(dirname "$0")" && pwd)"
executable=${mysos_dir}/../dist/mysos_test_client.pex

if [ ! -f ${executable} ]; then
  echo "${executable} doesn't exist. Build it first."
  exit 1
fi

${executable} \
  --api_host=${host} \
  --api_port=${port} \
  --cluster_user=${cluster_user} \
  --cluster=${cluster_name} \
  --num_nodes=${num_nodes}
