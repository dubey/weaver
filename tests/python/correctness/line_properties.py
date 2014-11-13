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
c = client.Client('127.0.0.1', 2002)

def line_requests(eprops, exp_reach):
    num_reach = 0
    rp = client.ReachParams(dest=nodes[num_nodes-1], edge_props=eprops)
    timer = time.time()
    for i in range(num_nodes-1):
        prog_args = [(nodes[i], rp)]
        response = c.run_reach_program(prog_args)
        #print 'done line req'
        if response.reachable:
            num_reach += 1
            if exp_reach == 0:
                print 'bad reach'
    timer = time.time() - timer
    assert (num_reach == exp_reach), 'expected reachable ' + str(exp_reach) + ', actually reachable ' + str(num_reach)

c.begin_tx()
for i in range(num_nodes):
    nodes.append(c.create_node())
assert c.end_tx(), 'create node tx'

c.begin_tx()
for i in range(num_nodes-1):
    edges.append(c.create_edge(nodes[i], nodes[i+1]))
assert c.end_tx(), 'create edge tx'

node_count = c.get_node_count()
print 'Node count:'
for cnt in node_count:
    print str(cnt)

#dummy = raw_input('press a key when shard is killed ')

print 'Now testing without edge props'
line_requests([], num_nodes-1)
print 'Done testing without edge props'

print 'Now testing with edge props'
line_requests([('color','blue')], 0)
print 'Done testing with edge props'

# adding edge props
c.begin_tx()
for i in range(num_nodes-1):
    c.set_edge_property(nodes[i], edges[i], 'color', 'blue')
assert c.end_tx(), 'set edge props tx'

print 'Now testing without edge props'
line_requests([], num_nodes-1)

print 'Now testing with edge props'
line_requests([('color','blue')], num_nodes-1)

print 'Now testing with WRONG edge props'
line_requests([('color','abcd')], 0)

print 'Now testing with WRONG edge props'
line_requests([('color','abc')], 0)

print 'Pass line_properties.'
