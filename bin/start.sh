#!/bin/sh

sudo yum --assumeyes install git tmux
wget https://downloads.typesafe.com/typesafe-activator/1.3.7/typesafe-activator-1.3.7.zip
git clone git@github.com:derekedw/spark4n6.git spark4n6

set +x
. ~/spark4n6/bin/functions.sh
tmux new-session ~/spark4n6/bin/test.sh
tmux split-window -h ~/spark4n6/bin/build.sh

