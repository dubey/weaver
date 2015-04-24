#! /bin/bash
#
# setup.sh
# Copyright (C) 2014 Ayush Dubey <dubey@cs.cornell.edu>
#
# See the LICENSE file for licensing agreement
#

echo 'Setup weaver support infrastructure.'
config_file="$WEAVER_SRCDIR"/conf/weaver.yaml
WEAVER_BUILDDIR="$WEAVER_BUILDDIR" "$WEAVER_SRCDIR"/startup_scripts/start_weaver.sh $config_file
echo 'Start weaver-shard.'
weaver shard --config-file=$config_file &
sleep 1
num_vts=$(weaver-parse-config --config-file=$config_file -c num_vts)
for i in $(seq 1 $num_vts); do
    echo 'Start weaver-timestamper.'
    weaver timestamper --config-file=$config_file &
done
