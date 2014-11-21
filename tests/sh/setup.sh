#! /bin/bash
#
# setup.sh
# Copyright (C) 2014 Ayush Dubey <dubey@cs.cornell.edu>
#
# See the LICENSE file for licensing agreement
#

echo 'Setup weaver support infrastructure.'
"$WEAVER_SRCDIR"/startup_scripts/start_weaver_support.sh
echo 'Start weaver-shard.'
weaver-shard &
sleep 1
echo 'Start weaver-timestamper.'
weaver-timestamper &
