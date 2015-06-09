#! /usr/bin/env python
# 
# ===============================================================
#    Description:  Launch or kill Weaver cluster
# 
#         Author:  Ayush Dubey, dubey@cs.cornell.edu
# 
# Copyright (C) 2014, Cornell University, see the LICENSE file
#                     for licensing agreement
# ===============================================================
# 

import subprocess
import argparse
import yaml
import time


def execute_command(command):
    try:
        return subprocess.check_output(command, shell=False)
    except subprocess.CalledProcessError as e:
        print 'CalledProcessError, returncode=' + str(e.returncode) + ', output=' + str(e.output)

def execute_remote_command(command, ip_addr, sleep=False):
    ssh_command = ['ssh', ip_addr]
    ssh_command.extend(command)
    output = execute_command(ssh_command)
    if sleep:
        time.sleep(1)
    return output

def main():
    parser = argparse.ArgumentParser(description='Manage Weaver cluster as specified by the yaml cluster file.  Default action is to launch new Weaver processes.')
    parser.add_argument('-c', '--cluster-file', default='cluster.yaml', help='yaml file describing the cluster')
    parser.add_argument('-g', '--graph-file', help='initial bulk load graph file')
    parser.add_argument('-n', '--num-shards', default='-1', help='bulk load num shards (if different from len(shards))')
    parser.add_argument('-k', '--kill', help='kill weaver cluster processes', action='store_true')

    args = parser.parse_args()

    with open(args.cluster_file, 'r') as f:
        cluster = yaml.load(f)
    print 'Cluster = ' + str(cluster)
    shards = []
    vts = []
    if 'shards' in cluster:
        shards = cluster['shards']
    if 'timestampers' in cluster:
        vts = cluster['timestampers']
    if args.num_shards == '-1':
        bulk_load_num_shards = len(shards)
    else:
        bulk_load_num_shards = int(args.num_shards)
    print 'Bulk load #shards=' + str(bulk_load_num_shards)

    shard_count = 0
    for s in shards:
        if args.kill:
            execute_remote_command(['pidof', 'weaver-shard',
                                    '|', 'xargs', 'kill', '-9'], s)
            print 'Killed shard ' + str(shard_count)
        else:
            execute_remote_command(['mkdir', '-p', '~/weaver_runtime/shard'], s)
            command = ['cd', '/local/dubey', '&&',
                       'weaver', 'shard',
                       '-l', s,
                       '--log-file', '~/weaver_runtime/shard/'+str(shard_count)+'.log']
            if args.graph_file is not None:
                command.extend(['--graph-format', 'graphml',
                                '--graph-file', args.graph_file + '_' + str(shard_count),
                                '--bulk-load-num-shards', str(bulk_load_num_shards)])
            command.append('> /dev/null 2>&1 &')
            execute_remote_command(command, s, True)
            print 'Launched shard ' + str(shard_count)
        shard_count += 1

    vt_count = 0
    for vt in vts:
        if args.kill:
            execute_remote_command(['pidof', 'weaver-timestamper',
                                    '|', 'xargs', 'kill', '-9'], vt)
            print 'Killed vt ' + str(vt_count)
        else:
            execute_remote_command(['mkdir', '-p', '~/weaver_runtime/timestamper'], vt)
            command = ['cd', '/local/dubey', '&&',
                       'weaver', 'timestamper',
                       '-l', vt,
                       '--log-file', '~/weaver_runtime/timestamper/'+str(vt_count)+'.log']
            command.append('> /dev/null 2>&1 &')
            execute_remote_command(command, vt)
            print 'Launched vt ' + str(vt_count)
        vt_count += 1


if __name__ == '__main__':
    main()
