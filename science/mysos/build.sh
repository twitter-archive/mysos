#!/bin/bash

mysos_dir="$(cd "$(dirname "$0")" && pwd)"

pushd ${mysos_dir}/..
PY=2.7 ./pants tests/python/twitter/mysos/executor:test_executor_builds
PY=2.7 ./pants tests/python/twitter/mysos/functional:test_functional_builds
PY=2.7 ./pants tests/python/twitter/mysos/scheduler:test_scheduler_builds
popd
