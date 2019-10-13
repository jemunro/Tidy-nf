#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd -P )"
CP="$DIR/src/main/groovy:$DIR/src/tests/groovy"
exec groovy -cp "$CP" "$DIR/src/tests/groovy/run_tests.groovy"