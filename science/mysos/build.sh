#!/bin/bash

pushd ..
PY=2.7 ./pants tests/python/twitter/mysos/scheduler:test_scheduler_builds
PY=2.7 ./pants tests/python/twitter/mysos/executor:test_executor_builds
popd
