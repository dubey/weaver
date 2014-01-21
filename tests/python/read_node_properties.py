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
c.set_edge_property(tx_id, node_id, 'color', 'blue')
c.end_tx(tx_id)

rp = client.ReadNodePropsParams(vt_id=coord_id)
prog_args = [(node_id, rp)]
response = c.run_reach_program(prog_args)
print response.node_props
