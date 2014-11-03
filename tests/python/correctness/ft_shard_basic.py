#! /usr/bin/env python
# 
# ===============================================================
#    Description:  Basic fault tolerance test:
#                  create a graph, kill some primary shards, read
#                  from backups. 
# 
#        Created:  2014-06-12 10:21:05
# 
#         Author:  Ayush Dubey, dubey@cs.cornell.edu
# 
# Copyright (C) 2013, Cornell University, see the LICENSE file
#                     for licensing agreement
# ===============================================================
# 

import sys
import time
import weaver.client as client

# grid graph params
grid_sz = 10 # grid_sz*grid_sz nodes
coord_id = 0
c = client.Client('127.0.0.1', 2002)

# create grid graph
nodes = [[0] * grid_sz] * grid_sz
edges = {}
for i in range(grid_sz):
    for j in range(grid_sz):
        c.begin_tx()
        nodes[i][j] = c.create_node()
        edges[nodes[i][j]] = []
        if i > 0:
            e = c.create_edge(nodes[i-1][j], nodes[i][j])
            edges[nodes[i-1][j]].append(e)
            e = c.create_edge(nodes[i][j], nodes[i-1][j])
            edges[nodes[i][j]].append(e)
        if j > 0:
            e = c.create_edge(nodes[i][j-1], nodes[i][j])
            edges[nodes[i][j-1]].append(e)
            e = c.create_edge(nodes[i][j], nodes[i][j-1])
            edges[nodes[i][j]].append(e)
        c.end_tx()

# read the graph written
param = client.ReadNEdgesParams()
for i in range(grid_sz):
    for j in range(grid_sz):
        prog_args = [(nodes[i][j], param)]
        response = c.read_n_edges(prog_args)
        for e in response.return_edges:
            assert (e in edges[nodes[i][j]])

dummy = raw_input('Press a key when shards and/or timestampers are killed ')

# read the graph written
param = client.ReadNEdgesParams()
for i in range(grid_sz):
    for j in range(grid_sz):
        prog_args = [(nodes[i][j], param)]
        response = c.read_n_edges(prog_args)
        for e in response.return_edges:
            assert (e in edges[nodes[i][j]])

print 'Pass ft_basic.'
