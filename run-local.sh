#!/bin/bash
if [ $# -lt 3 ]; then
	echo "Usage: cat DOCUMENT | run-local.sh [map|value] htable cf"
	echo "Ex:    cat ../1 | run-local.sh map default.htable cf"
	echo "Ex:    cat ../2 | run-local.sh value default.htable cf:cq"
	exit 1
fi
CP="clj-hbase-mapper-example-0.0.1-standalone.jar"
java -cp "/etc/hadoop/conf:/usr/lib/hadoop/*:/usr/lib/hadoop/lib/*:/etc/hbase/conf:/usr/lib/hbase/hbase.jar:/usr/lib/hbase/lib/*:$CP" -server -XX:+DisableExplicitGC -XX:+UseConcMarkSweepGC -Xms512M -Xmx1024M  clj_hbase_mapper_example.local $*
