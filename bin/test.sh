#!/bin/sh

set +x
	. ~/spark4n6/bin/functions.sh
	aws s3api list-buckets --output text | grep "BUCKETS"
	printf '%s' 'Bucket name: '
	read bucket
	aws s3 ls s3://${bucket} --recursive | grep ".E01" | grep -v ".txt"
	printf '%s' 'Path to test image: '
	read testImagePath
	testImageDir=$(dirname ${testImagePath})
	testImageBase=$(basename ${testImagePath} | sed -e 's/\.E01//')
	if hadoop fs -test -d /user/hadoop; then
		echo /user/hadoop is a directory
	else
		hadoop fs -mkdir /user/hadoop
	fi
	hadoop distcp \
		s3n://${bucket}/${testImageDir}/${testImageBase}.E* \
		/user/hadoop
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
			run_test 8 $i $(( 2 ** j )) ${testImageBase}.E01
			sleep 300
			j=$(( j + 1 ))
		    done
		    i=$(( i + 1 ))
		done
	done

