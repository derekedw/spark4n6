#!/bin/sh

sudo yum --assumeyes install git tmux
wget https://downloads.typesafe.com/typesafe-activator/1.3.7/typesafe-activator-1.3.7.zip
git clone https://github.com/derekedw/spark4n6.git spark4n6

set +x
export testImage=$1
tmux new-session "~/spark4n6/bin/test.sh ${testImage}"
tmux split-window -h ~/spark4n6/bin/build.sh
