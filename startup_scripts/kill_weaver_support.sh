#! /bin/bash
#
# stop_weaver_support.sh
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
ssh $hyperdex_coord_ipaddr "killall replicant-daemon"

num_daemons=${#hyperdex_daemons_ipaddr[*]}
for i in $(seq 1 $num_daemons);
do
    idx=$(($i-1))
    ipaddr=${hyperdex_daemons_ipaddr[$idx]}
    echo "Killing all HyperDex daemons at $ipaddr"
    ssh $ipaddr "killall hyperdex-daemon"
done

num_sm_daemons=${#server_manager_ipaddr[*]}
for i in $(seq 1 $num_sm_daemons);
do
    idx=$(($i-1))
    ipaddr=${server_manager_ipaddr[$idx]}
    echo "Killing all replicant daemons at $ipaddr"
    ssh $ipaddr "killall replicant-daemon"
done

num_kronos_daemons=${#kronos_ipaddr[*]}
for i in $(seq 1 $num_kronos_daemons);
do
    idx=$(($i-1))
    ipaddr=${kronos_ipaddr[$idx]}
    echo "Killing all replicant daemons at $ipaddr"
    ssh $ipaddr "killall replicant-daemon"
done
