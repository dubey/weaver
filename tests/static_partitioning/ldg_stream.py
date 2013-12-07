#! /usr/bin/env python

import sys
import random

# adds node attribute of which shard node should be placed on
num_shards = 8
capacity = 12000
assignments = dict()
shard_sizes = [0] * num_shards
G = {}

def load(argv):
    assert(len(argv) == 2)
    print 'loading graph from file'
    inputfile = open(argv[1], 'r')
    for line in inputfile:
        if line[0] == '#': # ignore comments
            continue
        edge = line.split()
        assert(len(edge) == 2) 
        n0 = int(edge[0])
        n1 = int(edge[1])
        if n0 not in G:
            G[n0] = []
        if n1 not in G:
            G[n1] = []
        G[n0].append(n1)
    inputfile.close()

def get_balanced_assignment(tied_shards):
    min_size = shard_sizes[tied_shards[0]] #pick one as min
    min_indices = []
    for s in tied_shards:
        if shard_sizes[s] < min_size:
            min_size = shard_sizes[s]
            min_indices = [s]
        elif shard_sizes[s] == min_size:
            min_indices.append(s)

    assert(len(min_indices) > 0)
    return random.choice(min_indices)


def penalty(shard):
    return 1.0 - (float(shard_sizes[shard])/float(capacity))

def get_ldg_assignment(nbr_iter):
    num_intersections = [0] * num_shards
    for nbr in nbr_iter:
        if nbr in assignments:
            num_intersections [assignments[nbr]] += 1

    arg_max = 0.0
    max_indices = []
    for i in range(num_shards):
        val = (float(num_intersections[i])*penalty(i))
        if arg_max < val:
            arg_max = val
            max_indices = [i]
        elif arg_max == val:
            max_indices.append(i)

    assert(len(max_indices) > 0)
    if len(max_indices) is 1:
        return max_indices[0]
    else:
        return get_balanced_assignment(max_indices)



print 'partitioning graph onto ' + str(num_shards) + ' shards using LDG with a capacity constant of ' + str(capacity)
load(sys.argv)
for n in G:
    put_on_shard = get_ldg_assignment(G[n])
    assignments[n] = put_on_shard 
    shard_sizes[put_on_shard] += 1

'''
colors = [float(assignments[n])/float(num_shards) for n in G.nodes()] 
print 'trying to draw graph...'
nx.draw_circular(G, node_color=colors)
plt.show()
'''

fname = sys.argv[1].rsplit('.',1)
if len(fname) == 1:
    fileout = open(fname[0] + '-partitioned.', 'w')
else:
    fileout = open(fname[0] + '-partitioned.' + fname[1], 'w')
fileout.write('#' + str(len(assignments)) + '\n')
for (k,v) in assignments.iteritems():
    fileout.write(str(k) + ' '  + str(v) + '\n')
for n in G:
    for nbr in G[n]:
        fileout.write(str(n) + ' ' + str(nbr) + '\n')
fileout.close()
print 'finshed writing assignments'
