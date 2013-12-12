#! /usr/bin/env python

import sys
import random
import math
from sets import Set

# adds node attribute of which shard node should be placed on
num_shards = 8
num_runs = 10
capacity = 84000/num_shards
assignments = dict()
shard_sizes = [0] * num_shards
LDG = True
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
            G[n0] = Set([])
        if n1 not in G:
            G[n1] = Set([])
        G[n0].add(n1)
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

def get_intersection_scores(node):
    shard_scores = [0] * num_shards
    for nbr in G[node]:
        if nbr in assignments:
            shard_scores[assignments[nbr]] += 1
    return shard_scores

def clustering_multiplier(num_mutual_friends):
    return math.log(2 + num_mutual_friends)

def calc_mutual_friends(n1, n2):
    return len(G[n1] & G[n2])

def get_clustering_scores(node):
    shard_scores = [0] * num_shards
    for nbr in G[node]:
        if nbr in assignments:
            mutual_friends = calc_mutual_friends(node, nbr)
            shard_scores[assignments[nbr]] += clustering_multiplier(mutual_friends)
    return shard_scores

def get_ldg_assignment(node):
    if LDG:
        shard_scores = get_intersection_scores(node)
    else:
        shard_scores = get_clustering_scores(node)

    arg_max = 0.0
    max_indices = []
    for i in range(num_shards):
        val = (float(shard_scores[i])*penalty(i))
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

def get_hash_assignment(node):
    return node % num_shards

print 'partitioning graph onto ' + str(num_shards) + ' shards using LDG with a capacity constant of ' + str(capacity)
load(sys.argv)
for run in range(num_runs):
    moved = 0
    for n in G:
        orig_loc = -1
        if n in assignments:
            shard_sizes[assignments[n]] -= 1
            orig_loc = assignments[n]
        #put_on_shard = get_ldg_assignment(n)
        put_on_shard = get_hash_assignment(n)
        assignments[n] = put_on_shard 
        shard_sizes[put_on_shard] += 1
        if orig_loc != -1 and orig_loc != put_on_shard:
            moved += 1
    print 'Completed run ' + str(run) + ', moved node count = ' + str(moved)
    print shard_sizes

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
        line = str(n) + ' ' + str(nbr)
        if random.random() > 0.8:
            line += ' color blue\n'
        else:
            line += '\n'
        fileout.write(line)
fileout.close()
print 'finshed writing assignments'
