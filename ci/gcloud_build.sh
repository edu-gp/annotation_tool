#!/usr/bin/env bash

TARGETS=()
OPTS=()
while test $# -gt 0; do
  case "$1" in
    -h|--help)
      echo "Usage ci/gcloud_build.sh [gcloud ops] -t TARGET [-t TARGET [-t TARGET]]"
      echo "This script is used to easily submit a gcloud build job from"
      echo "your local development environment. It should be run from"
      echo "the repository's root directory."
      echo ""
      echo -e "-t\tAdd a build target. At least one required."
      echo -e "  \tCheck out possible values in Dockerfile."
      echo -e "  \tlocal, staging, production are some examples."
      echo ""
      echo -e "All other arguments are passed directly to gcloud build. If you"
      echo -e "are building multiple targets --async can be helpful to build"
      echo -e "them simultaneously."
      echo ""
      exit 0
      ;;
    -t)
      if [ -n "$2" ] && [ ${2:0:1} != "-" ]; then
        TARGETS+=( "$2" )
        shift 2
      else
        echo "Error: Missing argument for $1" >&2
        exit 1
      fi
      ;;
    *) # preserve positional arguments
      OPTS+=( "$1" )
      shift
      ;;
  esac
done

if [ ${#TARGETS[@]} -eq 0 ]; then
  echo "At least one target is required"
  echo "Check out possible values in Dockerfile."
  echo "local, staging, production are some"
  echo "examples."
  exit 1
fi

COMMIT_SHA=$(git rev-parse $(git log -1 --pretty=format:%h))
SHORT_SHA=$(git rev-parse --short $(git log -1 --pretty=format:%h))
BRANCH_NAME=$(git rev-parse --abbrev-ref HEAD)

echo "Building for ${#TARGETS[@]} targets: ${TARGETS[@]}"
for TARGET in ${TARGETS[@]}; do
  gcloud builds submit ${OPTS[*]} --substitutions=SHORT_SHA="$SHORT_SHA",_TARGET="$TARGET",BRANCH_NAME="$BRANCH_NAME",COMMIT_SHA="$COMMIT_SHA" .
done
