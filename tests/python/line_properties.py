#! /usr/bin/env python
# 
# ===============================================================
#    Description:  Basic test for node/edge properties API. 
# 
#        Created:  2013-12-17 14:50:18
# 
#         Author:  Ayush Dubey, dubey@cs.cornell.edu
# 
# Copyright (C) 2013, Cornell University, see the LICENSE file
#                     for licensing agreement
# ===============================================================
# 

import sys
import time
sys.path.append('../../bindings/python')

import client

# creating line graph
nodes = []
edges = []
num_nodes = 200
coord_id = 0
c = client.Client(client._CLIENT_ID, coord_id)

def line_requests(eprops, exp_reach):
    num_reach = 0
    rp = client.ReachParams(dest=nodes[num_nodes-1], edge_props=eprops)
    timer = time.time()
    for i in range(num_nodes-1):
        prog_args = [(nodes[i], rp)]
        response = c.run_reach_program(prog_args)
        if response.reachable:
            num_reach += 1
    timer = time.time() - timer
    print 'Num reachable ' + str(num_reach) + ', expected ' + str(exp_reach) + ', time taken = ' + str(timer)

tx_id = c.begin_tx()
for i in range(num_nodes):
    nodes.append(c.create_node(tx_id))
c.end_tx(tx_id)
tx_id = c.begin_tx()
for i in range(num_nodes-1):
    edges.append(c.create_edge(tx_id, nodes[i], nodes[i+1]))
c.end_tx(tx_id)
print 'Created graph\n'

print 'Now testing without edge props'
line_requests([], num_nodes-1)

print 'Now testing with edge props'
line_requests([('color','blue')], 0)

# adding edge props
tx_id = c.begin_tx()
for i in range(num_nodes-1):
    c.set_edge_property(tx_id, nodes[i], edges[i], 'color', 'blue')
c.end_tx(tx_id)
print 'All edge properties set'

print 'Now testing without edge props'
line_requests([], num_nodes-1)

print 'Now testing with edge props'
line_requests([('color','blue')], num_nodes-1)

print 'Now testing with WRONG edge props'
line_requests([('color','abcd')], 0)

print 'Now testing with WRONG edge props'
line_requests([('color','abc')], 0)
