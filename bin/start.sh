#!/bin/sh

function clean_db() {
    cat <<EOF | hbase shell
disable 'images'
drop 'images'
disable 'row-keys'
drop 'row-keys'
EOF
}

function run_test() {
    # $1 is the number of hosts in the cluster
    # $2 is the number of executors per host
    # $3 is the number of GB of RAM per executor
    if (( $1 > 0 && $2 > 0 && $3 > 0 )); then
        logfile="run$(( $1 * $2 ))x-$(( $3 ))gb.log"
        { time spark-submit --class com.edwardsit.spark4n6.EWFImage \
            --num-executors $(( $1 * $2 )) \
            --executor-memory "$(( $3 ))g" \
            ~/spark4n6/target/scala-2.10/spark4n6_2.10-1.0.jar load 500GB-CDrive.E01
        } 2>&1 | tee $logfile
	{ time spark-submit \
		--num-executors 24 --executor-memory 8g \
		--class com.edwardsit.spark4n6.Strings \
		target/scala-2.10/spark4n6_2.10-1.0.jar >/dev/null
	} 2>&1 | tee -a test.log
	{ time strings /mnt/500GB-CDrive.E01.dd >/dev/null
	} 2>&1 | tee -a test.log
	{ time java com.edwardsit.spark4n6.HBaseSHA1 500GB-CDrive.E01 
	} 2>&1 | tee -a test.log
	{ time sha1sum /mnt/500GB-CDrive.E01.dd
	} 2>&1 | tee -a test.log
        hadoop fs -copyFromLocal $logfile s3n://4n6/
    fi
}

export PROJECT_NAME=spark4n6
export PATH=${PATH}:~/activator-dist-1.3.7
export EXTRA_CLASSPATH=~/${PROJECT_NAME}/target/scala-2.10/spark4n6_2.10-1.0.jar
export CLASSPATH=$(hbase classpath):~/${PROJECT_NAME}/target/scala-2.10/spark4n6_2.10-1.0.jar

sudo yum --assumeyes install git tmux
wget https://downloads.typesafe.com/typesafe-activator/1.3.7/typesafe-activator-1.3.7.zip
git clone git@github.com:derekedw/${PROJECT_NAME}.git
rm test.log

tmux split-window -h '
    unzip activator-dist-1.3.7.zip
    (   cd ${PROJECT_NAME}
        activator package
        hadoop fs -copyFromLocal /usr/lib/pig/piggybank.jar .
        hadoop fs -copyFromLocal target/scala-2.10/spark4n6_2.10-1.0.jar .
    )
'

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

