#!/bin/sh

set +x
	. ~/spark4n6/bin/functions.sh
	while [ ! -d ~/spark4n6/ ]; do sleep 10; done
	while [ ! -f ~/spark4n6/target/scala-2.10/spark4n6_2.10-1.0.jar ]; do sleep 10; done

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

