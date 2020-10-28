#!/usr/bin/env sh

tests_list=$@
if [ -z $tests_list ]; then
  tests_list='tests'
fi

PYTEST_CMD="pytest --durations=30 $TEST_ARGS $tests_list"

echo $PYTEST_CMD
sh -c "$PYTEST_CMD"
