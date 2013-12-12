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
sys.path.append('../bindings/python')

import client

# create graph from file
print "graph should already exist, creating client"
coord_id = 0
c = client.Client(client._CLIENT_ID, coord_id)

dp = client.DijkstraParams(src_handle=0, dst_handle=5, edge_weight_name="weight", is_widest_path=False)
prog_args = [(0, dp)]
print "created prog args"
response = c.run_dijkstra_program(prog_args)
print "shortest path response was cost" + str(response.cost)
assert(response.cost == 17)

prog_args[0].second.is_widest_path = True
response = c.run_dijkstra_program(prog_args)
print "widest path response was cost" + str(response.cost)
assert(response.cost == 6)
