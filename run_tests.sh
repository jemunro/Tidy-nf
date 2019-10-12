#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd -P )"
CP="$DIR/src/main/groovy"
exec groovy -cp "$CP" "$DIR/src/tests/groovy/tests.groovy"