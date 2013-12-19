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
coord_id = 0
c = client.Client(client._CLIENT_ID, coord_id)

use_cache = True
num_nodes = 200
star_nodes = [] #[num_nodes];
star_edges = [] #[num_nodes*2];
edges_per_node = 6
#ring_edges = [] #[num_nodes*edges_per_node];

tx_id = c.begin_tx()

central_node = c.create_node(tx_id)
for i in range(num_nodes):
    star_nodes.append(c.create_node(tx_id))
c.end_tx(tx_id)
tx_id = c.begin_tx()
for i in range(num_nodes):
    c.create_edge(tx_id, central_node, star_nodes[i])
c.end_tx(tx_id)


cp = client.ClusteringParams(caching=use_cache)
prog_args = [(central_node, cp)]
response = c.run_clustering_program(prog_args)
print response.clustering_coeff;
assert(response.clustering_coeff == 0)
#connect star nodes back to center. Shouldn't change coefficient
tx_id = c.begin_tx()
for i in range(num_nodes):
    c.create_edge(tx_id, star_nodes[i], central_node)
c.end_tx(tx_id)

cp = client.ClusteringParams(caching=use_cache)
prog_args = [(central_node, cp)]
response = c.run_clustering_program(prog_args)
print response.clustering_coeff;
assert(response.clustering_coeff == 0)

# add edges in ring around star alternating with adding edge to extra node from center that would invalidate a cached value
extra_nodes = 0
for node_skip in range(1, edges_per_node+1):
    for i in range(num_nodes):
        tx_id = c.begin_tx()
        c.create_edge(tx_id, star_nodes[i], star_nodes[(i+node_skip)%num_nodes])
        c.end_tx(tx_id)

        numerator = float((node_skip-1)*num_nodes+i+1)
        denominator = float((num_nodes+extra_nodes)*(num_nodes+extra_nodes-1))

        cp = client.ClusteringParams(caching=use_cache)
        prog_args = [(central_node, cp)]
        response = c.run_clustering_program(prog_args)
        print "want " + str(numerator/denominator) + " got " + str(response.clustering_coeff)
        assert(response.clustering_coeff == (numerator/denominator))

        tx_id = c.begin_tx()
        extra_node = c.create_node(tx_id)
        c.create_edge(tx_id, central_node, extra_node)
        c.end_tx(tx_id)
        extra_nodes += 1

        numerator = float((node_skip-1)*num_nodes+i+1)
        denominator = float((num_nodes+extra_nodes)*(num_nodes+extra_nodes-1))

        cp = client.ClusteringParams(caching=use_cache)
        prog_args = [(central_node, cp)]
        response = c.run_clustering_program(prog_args)
        print "want " + str(numerator/denominator) + " got " + str(response.clustering_coeff)
        assert(response.clustering_coeff == (numerator/denominator))
print "passed all clustering tests"
