#! /usr/bin/env python
# 
# ===============================================================
#    Description:  Deletion fault tolerance test:
#                  create a graph, delete some nodes/edges,
#                  kill some primary shards, read from backups.
# 
#        Created:  2014-06-13 11:00:11
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
nodes = []
for i in range(grid_sz):
    nodes.append([0]*grid_sz)
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

print 'created graph'

# read the graph written
param = client.ReadNEdgesParams()
for i in range(grid_sz):
    for j in range(grid_sz):
        prog_args = [(nodes[i][j], param)]
        response = c.read_n_edges(prog_args)
        for e in response.return_edges:
            assert (e in edges[nodes[i][j]])

print 'read graph'

# delete some edges
for i in range(grid_sz):
    for j in range(grid_sz):
        n = nodes[i][j]
        if not edges[n]:
            continue
        c.begin_tx()
        e = edges[n][0]
        c.delete_edge(e, n)
        edges[n].remove(e)
        c.end_tx()

print 'deleted some edges'

# delete node at (0,0)
n = nodes[0][0]
c.begin_tx()
for e in edges[n]:
    c.delete_edge(e, n)
# in-edges for (0,0) have already been deleted
c.delete_node(n)
c.end_tx()

dummy = raw_input('Press a key when shards and/or timestampers are killed ')

node_count = c.get_node_count()
tot_count = 0
for cnt in node_count:
    tot_count += cnt
assert (tot_count == (grid_sz*grid_sz - 1))
print 'total node count = ' + str(tot_count)

# read the graph written
param = client.ReadNEdgesParams()
for i in range(grid_sz):
    for j in range(grid_sz):
        if i == 0 and j == 0:
            continue
        prog_args = [(nodes[i][j], param)]
        response = c.read_n_edges(prog_args)
        for e in response.return_edges:
            assert (e in edges[nodes[i][j]])

print 'Pass ft_shard_deletes.'
