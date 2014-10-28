#! /usr/bin/env python
# 
# ===============================================================
#    Description:  Basic test for reading node/edge properties. 
# 
#        Created:  2013-12-17 14:50:18
# 
#         Author:  Gregory D. Hill, gdh39@cornell.edu
# 
# Copyright (C) 2013, Cornell University, see the LICENSE file
#                     for licensing agreement
# ===============================================================
# 

import sys
import time

import weaver.client as client

coord_id = 0
c = client.Client('127.0.0.1', 2002)

# adding node properites
c.begin_tx()
node = c.create_node()
c.set_node_property(node, 'color', 'blue')
c.set_node_property(node, 'size', '27')
assert c.end_tx(), 'create node and set properties'

# reading node properites
rp = client.ReadNodePropsParams()
prog_args = [(node, rp)]
response = c.read_node_props(prog_args)
assert (('color', 'blue') in response.node_props and ('size', '27') in response.node_props and len(response.node_props) == 2)

prog_args[0][1].keys = ['color']
response = c.read_node_props(prog_args)
assert (('color', 'blue') in response.node_props and len(response.node_props) == 1)

c.begin_tx()
c.set_node_property(node, 'age', '37')
assert c.end_tx(), 'add age property'

prog_args[0][1].keys = ['size', 'age']
response = c.read_node_props(prog_args)
assert(('age', '37') in response.node_props and ('size', '27') in response.node_props and len(response.node_props) == 2)


# adding edges, edge properites

c.begin_tx()
neighbors = []
neighbors.append(c.create_node())
neighbors.append(c.create_node())
neighbors.append(c.create_node())
edges = []
edges.append(c.create_edge(node, neighbors[0]))
edges.append(c.create_edge(node, neighbors[1]))
edges.append(c.create_edge(node, neighbors[2]))
c.set_edge_property(node, edges[0], 'created', '1')
c.set_edge_property(node, edges[1], 'created', '2')
c.set_edge_property(node, edges[2], 'created', '3')
c.set_edge_property(node, edges[0], 'cost', '1')
c.set_edge_property(node, edges[1], 'cost', '2')
c.set_edge_property(node, edges[2], 'cost', '3')
assert c.end_tx(), 'add edges and corresponding properties'

# reading edge properties
rp = client.ReadEdgesPropsParams()
prog_args = [(node, rp)]
response = c.read_edges_props(prog_args)
prop_map = {}
for x in response.edges_props:
    prop_map[x[0]] = x[1]
for i in range(3):
    assert (edges[i] in prop_map), str(edges[i]) + ' in edge handles'
    assert ('created', str(i+1)) in prop_map[edges[i]], '(created, ' + str(i+1) + ') in edges_props'
    assert ('cost', str(i+1)) in prop_map[edges[i]], '(cost, ' + str(i+1) + ') in edges_props'

prog_args[0][1].keys = ['cost']
response = c.read_edges_props(prog_args)
prop_map = {}
for x in response.edges_props:
    prop_map[x[0]] = x[1]
for i in range(3):
    assert (edges[i] in prop_map), str(edges[i]) + ' in edge handles'
    assert ('created', str(i+1)) not in prop_map[edges[i]], '(created, ' + str(i+1) + ') in edges_props'
    assert ('cost', str(i+1)) in prop_map[edges[i]], '(cost, ' + str(i+1) + ') in edges_props'

print 'Pass read_properties.'
