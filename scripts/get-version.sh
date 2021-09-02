#!/usr/bin/env bash

set -e

VERSION=`mvn -q -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec | sed 's/^\(.*\)-SNAPSHOT/\1/'`
echo ${VERSION}
