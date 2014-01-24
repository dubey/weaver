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

# creating line graph
nodes = []
edges = []
num_nodes = 200
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
neighbors = []

# reading edge properties
neighbors = []
