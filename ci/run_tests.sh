#!/usr/bin/env sh

PYTEST_CMD="pytest --durations=30 $TEST_ARGS tests"

echo $PYTEST_CMD
sh -c "$PYTEST_CMD"
