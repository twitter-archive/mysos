#!/bin/sh

# This is an 'end-to-end' test that requires Mysos to be up and running in the Vagrant VM.

set -ue

host="192.168.33.17"
port=55001
cluster_name="test_cluster$((RANDOM % 1000))"
num_nodes=1
cluster_user="mysos"

HERE="$(cd "$(dirname "$0")" && pwd)"
executable=$HERE/../.tox/py27/bin/mysos_test_client

if [ ! -f ${executable} ]; then
  echo "${executable} doesn't exist. Build it first."
  exit 1
fi

${executable} create \
  --api_host=${host} \
  --api_port=${port} \
  --cluster_user=${cluster_user} \
  --cluster=${cluster_name} \
  --num_nodes=${num_nodes}

echo "Finished creating the cluster, now deleting it"

${executable} delete \
  --api_host=${host} \
  --api_port=${port} \
  --cluster=${cluster_name}

echo "Finished deleting the cluster"
