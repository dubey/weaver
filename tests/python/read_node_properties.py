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

tx_id = c.begin_tx()
node_id = c.create_node(tx_id)
c.set_node_property(tx_id, node_id, 'color', 'blue')
c.set_node_property(tx_id, node_id, 'size', '27')
c.end_tx(tx_id)

rp = client.ReadNodePropsParams(vt_id=coord_id)
prog_args = [(node_id, rp)]
response = c.read_node_props(prog_args)
print "All pairs: " + str(response.node_props)

rp = client.ReadNodePropsParams(vt_id=coord_id)
rp.keys = ['color']
prog_args = [(node_id, rp)]
response = c.read_node_props(prog_args)
print "Only lookup color : " + str(response.node_props)

tx_id = c.begin_tx()
c.set_node_property(tx_id, node_id, 'age', '37')
c.end_tx(tx_id)
print "adding 'age' property"

rp = client.ReadNodePropsParams(vt_id=coord_id)
rp.keys = ['size', 'age']
prog_args = [(node_id, rp)]
response = c.read_node_props(prog_args)
print "Lookup size, age : " + str(response.node_props)
