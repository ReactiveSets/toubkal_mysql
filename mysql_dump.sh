#!/bin/sh
# usage: mysql_dump.sh
#
# MySQL password must be in ./mysql.cnf:
#
# [client]
# password=your password
#
mysqldump \
   --defaults-extra-file=./mysql.cnf \
   --user=root \
   --host=localhost \
   --protocol=tcp \
   --port=3306 \
   --default-character-set=utf8 \
   --no-data \
   "toubkal_unit_tests" \
   > test/fixtures/test.sql
