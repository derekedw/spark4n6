#!/bin/sh

export PROJECT_NAME=spark4n6
export PATH=${PATH}:~/activator-dist-1.3.7
export EXTRA_CLASSPATH=~/${PROJECT_NAME}/target/scala-2.10/spark4n6_2.10-1.0.jar
export CLASSPATH=$(hbase classpath):~/${PROJECT_NAME}/target/scala-2.10/spark4n6_2.10-1.0.jar

sudo yum --assumeyes install git tmux
wget https://downloads.typesafe.com/typesafe-activator/1.3.7/typesafe-activator-1.3.7.zip
git clone git@github.com:derekedw/${PROJECT_NAME}.git
rm test.log

tmux new-session '
    unzip activator-dist-1.3.7.zip
    (   cd ${PROJECT_NAME}
        activator package
        hadoop fs -copyFromLocal /usr/lib/pig/piggybank.jar .
        hadoop fs -copyFromLocal target/scala-2.10/spark4n6_2.10-1.0.jar .
    )
'

tmux split-window -h '
	. ~/${PROJECT_NAME}/bin/functions.sh
	while [ ! -d ~/${PROJECT_NAME} ]; do sleep 10; done
	while [ ! -f ~/${PROJECT_NAME}/target/scala-2.10/spark4n6_2.10-1.0.jar ]; do sleep 10; done

	while true; do
		# Increment for the number of executors per host
		i=1
		while (( i <= 4 )); do
		    # Increment the RAM per executor (2, 4, 8GB)
		    j=1
		    while (( j <= 3 )); do
			clean_db
			run_test 8 $i $(( 2 ** j ))
			sleep 300
			j=$(( j + 1 ))
		    done
		    i=$(( i + 1 ))
		done
	done
'

