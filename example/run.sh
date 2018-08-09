#!/usr/bin/env bash

ROOT=$(cd $(dirname $0)/..; pwd)
EXAMPLE_ROOT=$ROOT/example
LOCAL_MAVEN_REPO=$ROOT/build/repo
OUTPUT="$1"

if [ -z "$OUTPUT" ]; then
    echo "[ERROR] Set output s3 URI as the first argument."
    exit 1
fi

(
  cd $EXAMPLE_ROOT

  ## to remove cache
  rm -rfv .digdag

  ## run
  digdag run example.dig -c allow.properties -p repos=${LOCAL_MAVEN_REPO} -p output=${OUTPUT} --no-save
)
