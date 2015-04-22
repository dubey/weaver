#! /bin/bash
#
# kill_weaver.sh
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

echo "Killing all replicant daemons at $hyperdex_coord_ipaddr"
if [ $hyperdex_coord_ipaddr = "127.0.0.1" ]; then
    pidof replicant-daemon | xargs kill -9
else
    ssh $hyperdex_coord_ipaddr "pidof replicant-daemon | xargs kill -9"
fi

num_daemons=${#hyperdex_daemons_ipaddr[*]}
for i in $(seq 1 $num_daemons);
do
    idx=$(($i-1))
    ipaddr=${hyperdex_daemons_ipaddr[$idx]}
    echo "Killing all HyperDex daemons at $ipaddr"
    if [ $ipaddr = "127.0.0.1" ]; then
        pidof hyperdex-daemon | xargs kill -9
    else
        ssh $ipaddr "pidof hyperdex-daemon | xargs kill -9"
    fi
done

num_sm_daemons=${#server_manager_ipaddr[*]}
for i in $(seq 1 $num_sm_daemons);
do
    idx=$(($i-1))
    ipaddr=${server_manager_ipaddr[$idx]}
    echo "Killing all replicant daemons at $ipaddr"
    if [ $ipaddr = "127.0.0.1" ]; then
        pidof replicant-daemon | xargs kill -9
    else
        ssh $ipaddr "pidof replicant-daemon | xargs kill -9"
    fi
done

num_kronos_daemons=${#kronos_ipaddr[*]}
for i in $(seq 1 $num_kronos_daemons);
do
    idx=$(($i-1))
    ipaddr=${kronos_ipaddr[$idx]}
    echo "Killing all replicant daemons at $ipaddr"
    if [ $ipaddr = "127.0.0.1" ]; then
        pidof replicant-daemon | xargs kill -9
    else
        ssh $ipaddr "pidof replicant-daemon | xargs kill -9"
    fi
done
