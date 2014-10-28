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

import weaver.client as client

# creating line graph
nodes = []
edges = []
num_nodes = 400
coord_id = 0
c = client.Client(client.CL_ID, coord_id)

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
    assert(num_reach == exp_reach)

c.begin_tx()
for i in range(num_nodes):
    nodes.append(c.create_node())
c.end_tx()

c.begin_tx()
for i in range(num_nodes-1):
    edges.append(c.create_edge(nodes[i], nodes[i+1]))
c.end_tx()
print 'Created graph'

node_count = c.get_node_count()
print 'Node count:'
for cnt in node_count:
    print str(cnt)

dummy = raw_input('press a key when shard is killed ')

print 'Now testing without edge props'
line_requests([], num_nodes-1)

print 'Now testing with edge props'
line_requests([('color','blue')], 0)

# adding edge props
c.begin_tx()
for i in range(num_nodes-1):
    c.set_edge_property(nodes[i], edges[i], 'color', 'blue')
c.end_tx()
print 'All edge properties set'

print 'Now testing without edge props'
line_requests([], num_nodes-1)

print 'Now testing with edge props'
line_requests([('color','blue')], num_nodes-1)

print 'Now testing with WRONG edge props'
line_requests([('color','abcd')], 0)

print 'Now testing with WRONG edge props'
line_requests([('color','abc')], 0)
