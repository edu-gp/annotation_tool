#!/usr/bin/env sh

tests_list=$@
if [ -z $tests_list ]; then
  tests_list='tests'
fi

if [ -z $ALCHEMY_CONFIG ]; then
  export ALCHEMY_CONFIG=/app/alchemy/config/test.py
fi

echo $ find /home/runner/work/ -name test.py
find /home/runner/work/ -name test.py
echo $ pwd
pwd
echo $ echo '$ALCHEMY_CONFIG'
echo $ALCHEMY_CONFIG

PYTEST_CMD="pytest --durations=30 $TEST_ARGS $tests_list"
echo $PYTEST_CMD
sh -c "$PYTEST_CMD"
