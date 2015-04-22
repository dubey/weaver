#! /bin/bash
#
# start_weaver.sh
# Copyright (C) 2014 Ayush Dubey <dubey@cs.cornell.edu>
#

# get config file location
if [ $# -eq 1 ]
then
    config_file_args="-f $1"
else
    config_file_args=""
fi

if [ -z "$WEAVER_BUILDDIR" ]
then
    if [ -e /usr/local/lib/libweaverservermanager.so ]
    then
        weaver_libdir=/usr/local/lib
    else
        if [ -e /usr/lib/libweaverservermanager.so ]
        then
            weaver_libdir=/usr/lib
        else
            echo 'Did not find libweaverservermanager.so at /usr/lib and /usr/local/lib.  Also, WEAVER_BUILDDIR not set.  Exiting now.'
            exit 1
        fi
    fi
else
    weaver_libdir="$WEAVER_BUILDDIR"/.libs
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
aux_index=($(weaver-parse-config -c aux_index $config_file_args))

rm_patterns="*.log *.sst *.old CURRENT  LOCK  LOG  MANIFEST*"

shopt -s nullglob
cd

# hyperdex
hyperdex_coord_dir="~/weaver_runtime/hyperdex/coord"
echo "Starting HyperDex coordinator at location $hyperdex_coord_ipaddr : $hyperdex_coord_port, data at $hyperdex_coord_dir"
if [ $hyperdex_coord_ipaddr = "127.0.0.1" ]; then
    mkdir -p $hyperdex_coord_dir
    cd $hyperdex_coord_dir
    rm -f $rm_patterns replicant-daemon-*
    hyperdex coordinator -l $hyperdex_coord_ipaddr -p $hyperdex_coord_port > /dev/null 2>&1
else
ssh $hyperdex_coord_ipaddr 'bash -s' << EOF
    shopt -s nullglob
    mkdir -p $hyperdex_coord_dir
    cd $hyperdex_coord_dir
    rm -f $rm_patterns replicant-daemon-*
    hyperdex coordinator -l $hyperdex_coord_ipaddr -p $hyperdex_coord_port > /dev/null 2>&1
EOF
fi
sleep 3

num_daemons=${#hyperdex_daemons_ipaddr[*]}
for i in $(seq 1 $num_daemons);
do
    idx=$(($i-1))
    ipaddr=${hyperdex_daemons_ipaddr[$idx]}
    port=${hyperdex_daemons_port[$idx]}
    directory="~/weaver_runtime/hyperdex/daemon$idx"
    echo "Starting HyperDex daemon $i at location $ipaddr : $port, data at $directory"
    if [ $ipaddr = "127.0.0.1" ]; then
        mkdir -p $directory
        cd $directory
        rm -f $rm_patterns hyperdex-daemon-*
        hyperdex daemon --listen=$ipaddr --listen-port=$port \
                        --coordinator=$hyperdex_coord_ipaddr --coordinator-port=$hyperdex_coord_port \
                        > /dev/null 2>&1
    else
    ssh $ipaddr 'bash -s' << EOF
        shopt -s nullglob
        mkdir -p $directory
        cd $directory
        rm -f $rm_patterns hyperdex-daemon-*
        hyperdex daemon --listen=$ipaddr --listen-port=$port \
                        --coordinator=$hyperdex_coord_ipaddr --coordinator-port=$hyperdex_coord_port \
                        > /dev/null 2>&1
EOF
    fi
done

sleep 3

echo 'Adding HyperDex spaces'

hyperdex add-space -h $hyperdex_coord_ipaddr -p $hyperdex_coord_port << EOF
space weaver_index_data
key idx
attributes
    string node,
    int shard
tolerate 2 failures
EOF

hyperdex add-space -h $hyperdex_coord_ipaddr -p $hyperdex_coord_port << EOF
space weaver_graph_data
key node
attributes
    int shard,
    string creat_time,
    list(string) properties,
    map(string, string) out_edges,
    int migr_status,
    string last_upd_clk,
    string restore_clk,
    set(string) aliases
tolerate 2 failures
EOF

hyperdex add-space -h $hyperdex_coord_ipaddr -p $hyperdex_coord_port << EOF
space weaver_tx_data
key tx_id
attributes
    int vt_id,
    string tx_data
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
    if [ $ipaddr = "127.0.0.1" ]; then
        mkdir -p $directory
        cd $directory
        rm -f *.log *.sst *.old CURRENT  LOCK  LOG  MANIFEST* replicant-daemon-*
        replicant daemon --daemon --listen $ipaddr --listen-port $port > /dev/null 2>&1
    else
    ssh $ipaddr 'bash -s' << EOF
        shopt -s nullglob
        mkdir -p $directory
        cd $directory
        rm -f *.log *.sst *.old CURRENT  LOCK  LOG  MANIFEST* replicant-daemon-*
        replicant daemon --daemon --listen $ipaddr --listen-port $port > /dev/null 2>&1
EOF
    fi
done
sleep 2

replicant new-object -h ${server_manager_ipaddr[0]} -p ${server_manager_port[0]} weaver "$weaver_libdir"/libweaverservermanager.so


# kronos
num_kronos_daemons=${#kronos_ipaddr[*]}
for i in $(seq 1 $num_kronos_daemons);
do
    idx=$(($i-1))
    ipaddr=${kronos_ipaddr[$idx]}
    port=${kronos_port[$idx]}
    directory="~/weaver_runtime/kronos/daemon$idx"
    echo "Starting Kronos at location  $ipaddr: $port, data at $directory"
    if [ $ipaddr = "127.0.0.1" ]; then
        mkdir -p $directory
        cd $directory
        rm -f *.log *.sst *.old CURRENT  LOCK  LOG  MANIFEST* replicant-daemon-*
        replicant daemon --daemon --listen $ipaddr --listen-port $port > /dev/null 2>&1
    else
    ssh $ipaddr 'bash -s' << EOF
        shopt -s nullglob
        mkdir -p $directory
        cd $directory
        rm -f *.log *.sst *.old CURRENT  LOCK  LOG  MANIFEST* replicant-daemon-*
        replicant daemon --daemon --listen $ipaddr --listen-port $port > /dev/null 2>&1
EOF
    fi
done
sleep 2

replicant new-object -h ${kronos_ipaddr[0]} -p ${kronos_port[0]} chronosd "$weaver_libdir"/libweaverchronosd.so

echo 'Done startup.'
