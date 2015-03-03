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

try:
    import weaver.client as client
except ImportError:
    import client

config_file=''

if len(sys.argv) > 1:
    config_file = sys.argv[1]

coord_id = 0
c = client.Client('127.0.0.1', 2002, config_file)

# adding node properites
c.begin_tx()
node = c.create_node()
c.set_node_property('color', 'blue', node)
c.set_node_property('size', '27', node)
c.end_tx()

# reading node properites
read_props = c.get_node_properties(node)
assert len(read_props) == 2
assert read_props['color'] == ['blue']
assert read_props['size'] == ['27']

c.begin_tx()
c.set_node_property('age', '37', node)
c.end_tx()

read_props = c.get_node_properties(node)
assert len(read_props) == 3
assert read_props['age'] == ['37']

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
c.set_edge_property(edges[0], 'created', '1')
c.set_edge_property(edges[1], 'created', '2')
c.set_edge_property(edges[2], 'created', '3')
c.set_edge_property(edges[0], 'cost', '1')
c.set_edge_property(edges[1], 'cost', '2')
c.set_edge_property(edges[2], 'cost', '3')
c.end_tx()

# reading edge properties
for i in range(3):
    read_edge = c.get_edge(edges[i], node)
    assert read_edge.handle == edges[i]
    assert read_edge.start_node == node
    assert read_edge.end_node == neighbors[i]
    assert read_edge.properties['created'] == [str(i+1)]
    assert read_edge.properties['cost'] == [str(i+1)]

print 'Pass read_properties.'
