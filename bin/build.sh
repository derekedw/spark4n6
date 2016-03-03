#!/bin/sh

set +x
	. ~/spark4n6/bin/functions.sh
    unzip ~/activator-dist-1.3.7.zip
    (   cd ~/spark4n6
        activator package
    )

