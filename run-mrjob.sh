#!/bin/bash
if [ $# -lt 4 ]; then
	echo "Usage: run-mrjob.sh [map|value] input-path htable cf"
	echo "Ex:    run-mrjob.sh map tmp/input.gz default.htable cf"
	echo "Ex:    run-mrjob.sh value tmp/input.gz default.htable cf:cq"
	exit 1
fi
CP="clj-hbase-mapper-example-0.0.1-standalone.jar"
java -cp "/etc/hadoop/conf:/usr/lib/hadoop/*:/usr/lib/hadoop/lib/*:/etc/hbase/conf:/usr/lib/hbase/hbase.jar:/usr/lib/hbase/lib/*:$CP" -server -XX:+DisableExplicitGC -XX:+UseConcMarkSweepGC -Xms512M -Xmx1024M  clj_hbase_mapper_example.mrjob $*
