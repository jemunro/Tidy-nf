#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd -P )"
exec "$DIR/src/tests/nextflow/test.nf"