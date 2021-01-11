#!/usr/bin/env bash

tests_list=$@
if [ -z $tests_list ]; then
  tests_list='tests'
fi

if [ "$COVERAGE" ]; then
  if [ -z $RUNNER_TEMP ]; then
    export RUNNER_TEMP=/tmp
  fi
  mkdir -p $RUNNER_TEMP/coverage
  COVERAGE_FNAME="$RUNNER_TEMP/coverage/test_coverage.xml"
  COVERAGE="-s --cov=alchemy --cov-report=xml:$COVERAGE_FNAME"
fi

PYTEST_CMD="pytest --durations=30 $TEST_ARGS $COVERAGE $tests_list"

echo $PYTEST_CMD
sh -c "$PYTEST_CMD"

