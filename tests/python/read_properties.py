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
sys.path.append('../../bindings/python')

import client

coord_id = 0
c = client.Client(client._CLIENT_ID, coord_id)

# adding node properites
tx_id = c.begin_tx()
node_id = c.create_node(tx_id)
c.set_node_property(tx_id, node_id, 'color', 'blue')
c.set_node_property(tx_id, node_id, 'size', '27')
c.end_tx(tx_id)

# reading node properites
rp = client.ReadNodePropsParams(vt_id=coord_id)
prog_args = [(node_id, rp)]
response = c.read_node_props(prog_args)
print "All pairs: " + str(response.node_props)
assert(('color', 'blue') in response.node_props and ('size', '27') in response.node_props and len(response.node_props) == 2)

prog_args[0][1].keys = ['color']
response = c.read_node_props(prog_args)
print "Only lookup color : " + str(response.node_props)
assert(('color', 'blue') in response.node_props and len(response.node_props) == 1)

tx_id = c.begin_tx()
c.set_node_property(tx_id, node_id, 'age', '37')
c.end_tx(tx_id)
print "adding 'age' property"

prog_args[0][1].keys = ['size', 'age']
response = c.read_node_props(prog_args)
print "Lookup size, age : " + str(response.node_props)
assert(('age', '37') in response.node_props and ('size', '27') in response.node_props and len(response.node_props) == 2)


# adding edges, edge properites

tx_id = c.begin_tx()
neighbors = []
neighbors.append(c.create_node(tx_id))
neighbors.append(c.create_node(tx_id))
neighbors.append(c.create_node(tx_id))
edges = []
edges.append(c.create_edge(tx_id, node_id, neighbors[0]))
edges.append(c.create_edge(tx_id, node_id, neighbors[1]))
edges.append(c.create_edge(tx_id, node_id, neighbors[2]))
c.set_edge_property(tx_id, node_id, edges[0], 'created', '1')
c.set_edge_property(tx_id, node_id, edges[1], 'created', '2')
c.set_edge_property(tx_id, node_id, edges[2], 'created', '3')
c.set_edge_property(tx_id, node_id, edges[0], 'cost', '7')
c.set_edge_property(tx_id, node_id, edges[1], 'cost', '8')
c.set_edge_property(tx_id, node_id, edges[2], 'cost', '9')
c.end_tx(tx_id)

# reading edge properties
rp = client.ReadEdgesPropsParams(vt_id=coord_id)
prog_args = [(node_id, rp)]
response = c.read_edges_props(prog_args)
print response.edges_props
