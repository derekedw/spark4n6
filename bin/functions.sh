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
            ~/${PROJECT_NAME}/target/scala-2.10/spark4n6_2.10-1.0.jar load 500GB-CDrive.E01
        } 2>&1 | tee -a $logfile
	{ time java com.edwardsit.spark4n6.HBaseSHA1 500GB-CDrive.E01 500GB-CDrive.E01.dd 
	} 2>&1 | tee -a $logfile
	sleep 180
	{ time spark-submit \
		--num-executors $(( $1 * $2 )) --executor-memory "$(( $3 ))g" \
		--class com.edwardsit.spark4n6.Strings \
		~/${PROJECT_NAME}/target/scala-2.10/spark4n6_2.10-1.0.jar >/dev/null
	} 2>&1 | tee -a $logfile
	sleep 180
	{ time strings /mnt/500GB-CDrive.E01.dd >/dev/null
	} 2>&1 | tee -a $logfile
	sleep 180
	{ time java com.edwardsit.spark4n6.HBaseSHA1 500GB-CDrive.E01 
	} 2>&1 | tee -a $logfile
	sleep 180
	{ time sha1sum /mnt/500GB-CDrive.E01.dd
	} 2>&1 | tee -a $logfile
    fi
}

