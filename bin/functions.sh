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
    # $4 is the test image path
    if (( $1 > 0 && $2 > 0 && $3 > 0 )); then
        logfile="run$(( $1 * $2 ))x-$(( $3 ))gb.log"
        { time spark-submit --class com.edwardsit.spark4n6.EWFImage \
            --num-executors $(( $1 * $2 )) \
            --executor-memory "$(( $3 ))g" \
            ~/spark4n6/target/scala-2.11/spark4n6_2.11-1.0.jar load $4
        } 2>&1 | tee -a $logfile
        echo EWFImage.load | tee -a $logfile
	{ time java com.edwardsit.spark4n6.HBaseSHA1 $4 $4.dd
	} 2>&1 | tee -a $logfile
        echo HBaseSHA1.dd | tee -a $logfile
	sleep 180
	{ time spark-submit \
		--num-executors $(( $1 * $2 )) --executor-memory "$(( $3 ))g" \
		--class com.edwardsit.spark4n6.Strings \
		~/spark4n6/target/scala-2.11/spark4n6_2.11-1.0.jar >/dev/null
	} 2>&1 | tee -a $logfile
        echo spark4n6.strings | tee -a $logfile
	sleep 180
	{ time strings /mnt/$4.dd >/dev/null
	} 2>&1 | tee -a $logfile
        echo strings.strings | tee -a $logfile
	sleep 180
	{ time java com.edwardsit.spark4n6.HBaseSHA1 $4
	} 2>&1 | tee -a $logfile
        echo HBaseSHA1.hash | tee -a $logfile
	sleep 180
	{ time sha1sum /mnt/$4.dd
	} 2>&1 | tee -a $logfile
        echo sha1sum.hash | tee -a $logfile
    fi
}

export PATH=${PATH}:~/activator-dist-1.3.7
export EXTRA_CLASSPATH=~/spark4n6/target/scala-2.11/spark4n6_2.11-1.0.jar
export CLASSPATH=$(hbase classpath):~/spark4n6/target/scala-2.11/spark4n6_2.11-1.0.jar

