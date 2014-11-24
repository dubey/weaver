#! /bin/bash
#
# clean.sh
# Copyright (C) 2014 Ayush Dubey <dubey@cs.cornell.edu>
#
# See the LICENSE file for licensing agreement
#

echo 'Killing shard and timestamper.'
pidof weaver-timestamper | xargs kill -9
pidof weaver-shard | xargs kill -9
sleep 1
echo 'Killing weaver support infrastructure.'
"$WEAVER_SRCDIR"/startup_scripts/kill_weaver_support.sh "$WEAVER_SRCDIR"/conf/weaver.yaml
echo 'Done cleanup.'
