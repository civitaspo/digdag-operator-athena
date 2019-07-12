#!/usr/bin/env bash

ROOT=$(cd $(dirname $0)/..; pwd)
EXAMPLE_ROOT=$ROOT/example
LOCAL_MAVEN_REPO=$ROOT/build/repo
DATABASE="$1"
OUTPUT="$2"
WORKGROUP="$3"
PARAM_OPTION=""

if [ -z "$DATABASE" ]; then
    echo "[ERROR] Set database as the first argument."
    exit 1
fi
if [ -z "$OUTPUT" ]; then
    echo "[ERROR] Set output s3 URI as the second argument."
    exit 1
fi
if [ -n "$WORKGROUP" ]; then
    PARAM_OPTION="-p workgroup=$WORKGROUP"
fi

(
  cd $EXAMPLE_ROOT

  ## to remove cache
  rm -rfv .digdag

  ## run
  digdag run example.dig -c allow.properties -p repos=${LOCAL_MAVEN_REPO} -p output=${OUTPUT} -p database=${DATABASE} $PARAM_OPTION --no-save
)
