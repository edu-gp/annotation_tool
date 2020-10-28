#!/usr/bin/env sh

tests_list=$@
if [ -z $tests_list ]; then
  tests_list='tests'
fi

export ALCHEMY_CONFIG=/app/alchemy/config/test.py
PYTEST_CMD="pytest --durations=30 $TEST_ARGS $tests_list"

echo $PYTEST_CMD
sh -c "$PYTEST_CMD"
