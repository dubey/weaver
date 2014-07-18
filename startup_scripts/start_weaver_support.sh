#! /bin/bash
#
# start_weaver_support.sh
# Copyright (C) 2014 Ayush Dubey <dubey@cs.cornell.edu>
#

# get config file location
if [ $# -eq 1 ]
then
    config_file_args="-f $1"
else
    config_file_args=""
fi

# get config params
hyperdex_coord_ipaddr=$(weaver-parse-config -c hyperdex_coord_ipaddr $config_file_args)
hyperdex_coord_port=$(weaver-parse-config -c hyperdex_coord_port $config_file_args)
hyperdex_daemons_ipaddr=($(weaver-parse-config -c hyperdex_daemons_ipaddr $config_file_args))
hyperdex_daemons_port=($(weaver-parse-config -c hyperdex_daemons_port $config_file_args))
server_manager_ipaddr=($(weaver-parse-config -c server_manager_ipaddr $config_file_args))
server_manager_port=($(weaver-parse-config -c server_manager_port $config_file_args))
kronos_ipaddr=($(weaver-parse-config -c kronos_ipaddr $config_file_args))
kronos_port=($(weaver-parse-config -c kronos_port $config_file_args))

rm_patterns="*.log *.sst *.old CURRENT  LOCK  LOG  MANIFEST*"

# hyperdex
hyperdex_coord_dir="~/weaver_runtime/hyperdex/coord"
echo "Starting HyperDex coordinator at location $hyperdex_coord_ipaddr : $hyperdex_coord_port, data at $hyperdex_coord_dir"
ssh $hyperdex_coord_ipaddr "\
    setopt nullglob && \
    mkdir -p $hyperdex_coord_dir && \
    cd $hyperdex_coord_dir && \
    rm -f $rm_patterns replicant-daemon-* && \
    hyperdex coordinator -l $hyperdex_coord_ipaddr -p $hyperdex_coord_port > /dev/null 2>&1"
sleep 2

num_daemons=${#hyperdex_daemons_ipaddr[*]}
for i in $(seq 1 $num_daemons);
do
    idx=$(($i-1))
    ipaddr=${hyperdex_daemons_ipaddr[$idx]}
    port=${hyperdex_daemons_port[$idx]}
    directory="~/weaver_runtime/hyperdex/daemon$idx"
    echo "Starting HyperDex daemon $i at location $ipaddr : $port, data at $directory"
    ssh $ipaddr "\
        setopt nullglob && \
        mkdir -p $directory && \
        cd $directory && \
        rm -f $rm_patterns hyperdex-daemon-* && \
        hyperdex daemon --listen=$ipaddr --listen-port=$port \
                        --coordinator=$hyperdex_coord_ipaddr --coordinator-port=$hyperdex_coord_port \
                        > /dev/null 2>&1"
done

sleep 1

echo 'Adding HyperDex spaces'

hyperdex add-space -h $hyperdex_coord_ipaddr -p $hyperdex_coord_port << EOF
space weaver_loc_mapping
key node
attributes
    int shard
subspace shard
create 8 partitions
tolerate 2 failures
EOF

hyperdex add-space -h $hyperdex_coord_ipaddr -p $hyperdex_coord_port << EOF
space weaver_client_mapping
key str_handle
attributes
    int handle
create 8 partitions
tolerate 2 failures
EOF

hyperdex add-space -h $hyperdex_coord_ipaddr -p $hyperdex_coord_port << EOF
space weaver_graph_data
key node
attributes
    string creat_time,
    string del_time,
    string properties,
    map(string, string) out_edges,
    set(int) in_nbrs,
    string tx_queue,
    int migr_status
tolerate 2 failures
EOF

hyperdex add-space -h $hyperdex_coord_ipaddr -p $hyperdex_coord_port << EOF
space weaver_shard_data
key shard
attributes
    map(int, int) qts,
    int migr_token
tolerate 2 failures
EOF

hyperdex add-space -h $hyperdex_coord_ipaddr -p $hyperdex_coord_port << EOF
space weaver_vt_tx_set_data
key vt
attributes
    set(int) tx_id_set
tolerate 2 failures
EOF

hyperdex add-space -h $hyperdex_coord_ipaddr -p $hyperdex_coord_port << EOF
space weaver_vt_tx_map_data
key tx_id
attributes
    string tx_data,
    int status
tolerate 2 failures
EOF


# server manager
num_sm_daemons=${#server_manager_ipaddr[*]}
for i in $(seq 1 $num_sm_daemons);
do
    idx=$(($i-1))
    ipaddr=${server_manager_ipaddr[$idx]}
    port=${server_manager_port[$idx]}
    directory="~/weaver_runtime/server_manager/daemon$idx"
    echo "Starting server manager at location  $ipaddr: $port, data at $directory"
    ssh $ipaddr "\
        setopt nullglob && \
        mkdir -p $directory && \
        cd $directory && \
        rm -f *.log *.sst *.old CURRENT  LOCK  LOG  MANIFEST* replicant-daemon-* && \
        replicant daemon --daemon --listen $ipaddr --listen-port $port > /dev/null 2>&1"
done
sleep 1

replicant new-object -h ${server_manager_ipaddr[0]} -p ${server_manager_port[0]} weaver /usr/local/lib/libweaverservermanager.so


# kronos
num_kronos_daemons=${#kronos_ipaddr[*]}
for i in $(seq 1 $num_kronos_daemons);
do
    idx=$(($i-1))
    ipaddr=${kronos_ipaddr[$idx]}
    port=${kronos_port[$idx]}
    directory="~/weaver_runtime/kronos/daemon$idx"
    echo "Starting Kronos at location  $ipaddr: $port, data at $directory"
    ssh $ipaddr "\
        setopt nullglob && \
        mkdir -p $directory && \
        cd $directory && \
        rm -f *.log *.sst *.old CURRENT  LOCK  LOG  MANIFEST* replicant-daemon-* && \
        replicant daemon --daemon --listen $ipaddr --listen-port $port > /dev/null 2>&1"
done
sleep 1

replicant new-object -h ${kronos_ipaddr[0]} -p ${kronos_port[0]} chronosd /usr/local/lib/libchronosd.so

echo 'Done startup.'
