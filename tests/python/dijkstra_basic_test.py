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
sys.path.append('../../bindings/python')

import client

# create graph from file
coord_id = 0
c = client.Client(client._CLIENT_ID, coord_id)

nodes = dict()

tx_id = c.begin_tx()
for i in range(6):
    nodes[i] = c.create_node(tx_id)

edge_id = c.create_edge(tx_id, nodes[0], nodes[1])
c.set_edge_property(tx_id, nodes[0], edge_id, 'weight', '6')

edge_id = c.create_edge(tx_id, nodes[0], nodes[2])
c.set_edge_property(tx_id, nodes[0], edge_id, 'weight', '5')

edge_id = c.create_edge(tx_id, nodes[1], nodes[3])
c.set_edge_property(tx_id, nodes[1], edge_id, 'weight', '6')

edge_id = c.create_edge(tx_id, nodes[1], nodes[4])
c.set_edge_property(tx_id, nodes[1], edge_id, 'weight', '7')

edge_id = c.create_edge(tx_id, nodes[2], nodes[4])
c.set_edge_property(tx_id, nodes[2], edge_id, 'weight', '6')

edge_id = c.create_edge(tx_id, nodes[3], nodes[2])
c.set_edge_property(tx_id, nodes[3], edge_id, 'weight', '6')

edge_id = c.create_edge(tx_id, nodes[3], nodes[5])
c.set_edge_property(tx_id, nodes[3], edge_id, 'weight', '8')

edge_id = c.create_edge(tx_id, nodes[4], nodes[5])
c.set_edge_property(tx_id, nodes[4], edge_id, 'weight', '6')

c.end_tx(tx_id)

dp = client.DijkstraParams(src_handle=nodes[0], dst_handle=nodes[5], edge_weight_name="weight", is_widest_path=False)
prog_args = [(nodes[0], dp)]

response = c.run_dijkstra_program(prog_args)
print "shortest path response was cost " + str(response.cost)
assert(response.cost == 17)

prog_args[0][1].is_widest_path = True
response = c.run_dijkstra_program(prog_args)
print "widest path response was cost " + str(response.cost)
assert(response.cost == 6)
