#! /bin/bash
#
# setup.sh
# Copyright (C) 2014 Ayush Dubey <dubey@cs.cornell.edu>
#
# See the LICENSE file for licensing agreement
#

echo 'Setup weaver support infrastructure.'
WEAVER_BUILDDIR="$WEAVER_BUILDDIR" "$WEAVER_SRCDIR"/startup_scripts/start_weaver_support.sh "$WEAVER_SRCDIR"/conf/weaver.yaml
echo 'Start weaver-shard.'
weaver-shard --config-file="$WEAVER_SRCDIR"/conf/weaver.yaml &
sleep 1
echo 'Start weaver-timestamper.'
weaver-timestamper --config-file="$WEAVER_SRCDIR"/conf/weaver.yaml &
