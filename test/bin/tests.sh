#!/bin/sh
# Usage:
#   ./test/bin/tests.sh [target]
#
# Where target can be:
#   empty-string or "all": all coffee source are compiled and executed
#   non-empty string: test/src/$1.coffee is compiled and test/lib/$1.js is executed

if [ "$1" = "" ]; then
  test=all
else
  test=$1
fi

echo tests $test
echo

coffee --version
which coffee
echo
echo "Compile Coffee tests"

mkdir -p test/lib

if [ $test = "all" ]; then
  coffee --map --output test/lib --compile test/src
else
  coffee --map --output test/lib --compile test/src/tests_utils.coffee
  coffee --map --output test/lib --compile test/src/$test.coffee
fi

test=test/lib/$test.js

echo tests $test
echo
echo "Run tests"
mocha -R mocha-unfunk-reporter $test
