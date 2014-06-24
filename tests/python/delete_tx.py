#! /usr/bin/env python
# 
# ===============================================================
#    Description:  Test deletes with multiple clients and
#                  timestampers. 
# 
#        Created:  2014-06-17 10:41:16
# 
#         Author:  Ayush Dubey, dubey@cs.cornell.edu
# 
# Copyright (C) 2013, Cornell University, see the LICENSE file
#                     for licensing agreement
# ===============================================================
# 

import sys
import time
import random
import threading
import weaver.client as client

# grid graph params
num_node_groups = 1000
nodes_per_group = 5
num_vts = 1
num_cls = 10
clients = []
for i in range(num_cls):
    clients.append(client.Client(client._CLIENT_ID + i, i % num_vts))

# create grid graph
nodes = [[0 for i in range(nodes_per_group)] for j in range(num_node_groups)]
create_cl = clients[0]
for i in range(num_node_groups):
    for j in range(nodes_per_group):
        create_cl.begin_tx()
        nodes[i][j] = create_cl.create_node()
        create_cl.end_tx()

print 'created graph'

# delete all nodes, add random delays between txs
def del_edges(my_id, cl, success):
    assert len(success) == num_node_groups
    for i in range(num_node_groups):
        cl.begin_tx()
        for j in range(nodes_per_group):
            n = nodes[i][j]
            cl.delete_node(n)
        assert not success[i]
        success[i] = cl.end_tx()
        if success[i]:
            print 'Client ' + str(my_id) + ': tx ' + str(i)
        delay = random.random()/10
        time.sleep(delay)

threads = []
success = [[False for i in range(num_node_groups)] for j in range(num_cls)]
for i in range(num_cls):
    thr = threading.Thread(target=del_edges, args=(i, clients[i], success[i]))
    thr.start()
    threads.append(thr)

num_finished = 0
for thr in threads:
    thr.join()
    num_finished += 1
    print 'Thread finished #' + str(num_finished)

# check that only one transaction per node succeeded
for i in range(num_node_groups):
    num_true = 0
    for j in range(num_cls):
        if success[j][i]:
            num_true += 1
    assert num_true < 2
