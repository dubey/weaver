# 
# ===============================================================
#    Description:  Line reachability program in Python.
# 
#        Created:  11/11/2013 03:17:55 PM
# 
#         Author:  Ayush Dubey, dubey@cs.cornell.edu
# 
# Copyright (C) 2013, Cornell University, see the LICENSE file
#                     for licensing agreement
# ===============================================================
# 

import sys

import weaver.client as client

# creating line graph
nodes = []
num_nodes = 200
coord_id = 0
c = client.Client('127.0.0.1', 2002)

c.begin_tx()
for i in range(num_nodes):
    nodes.append(c.create_node())
assert c.end_tx(), 'create nodes tx'

c.begin_tx()
for i in range(num_nodes-1):
    c.create_edge(nodes[i], nodes[i+1])
assert c.end_tx(), 'create edges tx'

rp = client.ReachParams(dest=nodes[num_nodes-1])
for i in range(num_nodes):
    prog_args = [(nodes[i], rp)]
    response = c.run_reach_program(prog_args)
    #if i == num_nodes/10:
    #    c.start_migration()
    assert response.reachable, 'From node ' + str(i) + ' to node ' + str(num_nodes-1) + ', reachable = ' + str(response.reachable)

rp.dest=nodes[0]
for i in range(1, num_nodes):
    prog_args = [(nodes[i], rp)]
    response = c.run_reach_program(prog_args)
    assert (not response.reachable), 'From node ' + str(i) + ' to node ' + str(0) + ', reachable = ' + str(response.reachable)

print 'Pass line_reachability.'
