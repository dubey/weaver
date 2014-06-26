#! /bin/bash
#
# start_weaver_support.sh
# Copyright (C) 2014 Ayush Dubey <dubey@cs.cornell.edu>
#

# hyperdex
mkdir -p hyperdex_start/coord
mkdir -p hyperdex_start/daemon
cd hyperdex_start

cd coord
rm -f *.log *.sst *.old CURRENT  LOCK  LOG  MANIFEST* replicant-daemon-*
hyperdex coordinator -l 127.0.0.1 -p 7982
sleep 2

cd ../daemon
rm -f *.log *.sst CURRENT  LOCK  LOG  MANIFEST-000002 hyperdex-daemon-*
hyperdex daemon --listen=127.0.0.1 --listen-port=8012 --coordinator=127.0.0.1 --coordinator-port=7982
sleep 1

cd ..
hyperdex add-space -h 127.0.0.1 -p 7982 << EOF
space weaver_loc_mapping
key node
attributes
    int shard
subspace shard
create 8 partitions
tolerate 2 failures
EOF

hyperdex add-space -h 127.0.0.1 -p 7982 << EOF
space weaver_graph_data
key node
attributes
    string creat_time,
    string del_time,
    string properties,
    map(int, string) out_edges,
    set(int) in_nbrs,
    string tx_queue,
    int migr_status
tolerate 2 failures
EOF

hyperdex add-space -h 127.0.0.1 -p 7982 << EOF
space weaver_shard_data
key shard
attributes
    map(int, int) qts,
    int migr_token
tolerate 2 failures
EOF

hyperdex add-space -h 127.0.0.1 -p 7982 << EOF
space weaver_vt_tx_set_data
key vt
attributes
    set(int) tx_id_set
tolerate 2 failures
EOF

hyperdex add-space -h 127.0.0.1 -p 7982 << EOF
space weaver_vt_tx_map_data
key tx_id
attributes
    string tx_data,
    int status
tolerate 2 failures
EOF


# server manager
cd ..
mkdir -p server_manager_start/daemon1
mkdir -p server_manager_start/client

cd server_manager_start
cd daemon1
rm -f *.log *.sst *.old CURRENT  LOCK  LOG  MANIFEST* replicant-daemon-*
replicant daemon --daemon --listen 127.0.0.1 --listen-port 2002
sleep 1

cd ../client
replicant new-object -h 127.0.0.1 -p 2002 weaver /usr/local/lib/libweaverservermanager.so


# kronos
cd ../..
mkdir -p kronos_start/daemon1
mkdir -p kronos_start/client

cd kronos_start
cd daemon1
rm -f *.log *.sst *.old CURRENT  LOCK  LOG  MANIFEST* replicant-daemon-*
replicant daemon --daemon --listen 127.0.0.1 --listen-port 1992
sleep 1

cd ../client
replicant new-object -h 127.0.0.1 -p 1992 chronosd /usr/local/lib/libchronosd.so

cd ../../
