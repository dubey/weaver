#! /usr/bin/env python
# 
# ===============================================================
#    Description:  Test permanent deletion of nodes/edges. 
# 
#        Created:  2014-01-21 15:18:28
# 
#         Author:  Ayush Dubey, dubey@cs.cornell.edu
# 
# Copyright (C) 2013, Cornell University, see the LICENSE file
#                     for licensing agreement
# ===============================================================
# 

import sys
sys.path.append('../../.libs')

import libclient as client
import time
import random
from test_base import test_graph

coord_id = 0
num_nodes = 10000
num_reqs = 100
clients = []
clients.append(client.Client(client._CLIENT_ID, coord_id))
tg = test_graph(clients, num_nodes, 2*num_nodes)

def reach_requests():
    rp = client.ReachParams()
    for i in range(num_reqs):
        rp.dest = tg.nodes[random.randint(0, num_nodes)]
        src = tg.nodes[random.randint(0, num_nodes)]
        prog_args = [(src, rp)]
        response = clients[0].run_reach_program(prog_args)
        if i % 5 == 0:
            print 'Completed ' + str(i) + ' requests'

print 'Created graph'
reach_requests()
# delete bunch of nodes now and redo reachability
for i in range(num_nodes/2):
    clients[0].begin_tx()
    clients[0].delete_node(tg.nodes[i])
    clients[0].end_tx()
tg.nodes = tg.nodes[num_nodes/2:]
num_nodes = num_nodes/2
print 'Done deletion'
time.sleep(5)
print 'Done sleep'

reach_requests()

node_count = clients[0].get_node_count()
print 'Node count per shard:'
for cnt in node_count:
    print str(cnt)
