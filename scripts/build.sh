#!/usr/bin/env bash

set -ex

echo "Releasing aop"

version=${1#v}
if [[ "x$version" == "x" ]]; then
  echo "You need give a version number of the aop"
  exit 1
fi

# Create a direcotry to save assets
ASSETS_DIR=release
mkdir $ASSETS_DIR

mvn clean install -DskipTests -Dmaven.wagon.http.retryHandler.count=3
mv amqp-impl/target/pulsar-protocol-handler-amqp-*.nar  ./$ASSETS_DIR
cp README.md ./$ASSETS_DIR/pulsar-protocol-handler-amqp-readme.md
