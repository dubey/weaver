#! /usr/bin/env python
# 
# ===============================================================
#    Description:  Create a line graph.  Then delete all edges
#                  and connect consecutive vertices i s.t.
#                  i == 0 mod 2.  Then delete all edges and
#                  repeat for i == 0 mod 3.  Continue till V/2.
# 
#        Created:  2014-10-29 10:19:31
# 
#         Author:  Ayush Dubey, dubey@cs.cornell.edu
# 
# Copyright (C) 2013, Cornell University, see the LICENSE file
#                     for licensing agreement
# ===============================================================
#

import random
import threading
import weaver.client as client

num_nodes = 200

# delete current edges
# create new edges between vertices (0,new_mod),(new_mod, 2*new_mod),...
def remod_ring(c, edges, old_mod, new_mod):
    global num_nodes
    node = 0

    c.begin_tx()
    for e in edges:
        c.delete_edge(e, str(node))
        node += old_mod
    assert c.end_tx(), 'remod ring delete tx'

    del edges[:]
    
    c.begin_tx()
    for i in range(0, num_nodes-new_mod, new_mod):
        edges.append(c.create_edge(str(i), str(i+new_mod)))
    #print 'calling end_tx'
    assert c.end_tx(), 'remod ring create tx'
    #print 'done end_tx'
    #print 'remod ring from ' + str(old_mod) + ' to ' + str(new_mod)


# get path between src and dst vertices
# assert that path contains vertices in an arithmetic progression
def traverse_ring(c, src, dst):
    rp = client.ReachParams(dest=dst)
    #print 'calling run_prog'
    response = c.run_reach_program([(src, rp)])
    #print 'done run_prog'
    if response.reachable:
        assert (len(response.path) > 1), 'reachable path length = ' + str(len(response.path))
        diff = abs(int(response.path[1]) - int(response.path[0]))
        for i in range(len(response.path)-1):
            n1 = int(response.path[i])
            n2 = int(response.path[i+1])
            cur_diff = abs(n1-n2)
            assert (cur_diff == diff), 'diff = ' + str(cur_diff) + ', expected = ' + str(diff) + ', nodes: ' + response.path[i] + ',' + response.path[i+1]

def read_loop(c, loops):
    for i in range(loops):
        n1 = random.randrange(num_nodes)
        n2 = n1
        while n2 == n1:
            n2 = random.randrange(num_nodes)
        src = min(n1,n2)
        dst = max(n1,n2)
        traverse_ring(c, str(src), str(dst))
        #if i % 100 == 0:
            #print 'read loop progress ' + str(i)

num_readers = 20
readers = []
for i in range(num_readers):
    readers.append(client.Client('127.0.0.1', 2002))
writer = client.Client('127.0.0.1', 2002)

writer.begin_tx()
for i in range(num_nodes):
    writer.create_node(str(i))
assert writer.end_tx(), 'create nodes tx'

edges = []
writer.begin_tx()
for i in range(num_nodes-1):
    edges.append(writer.create_edge(str(i), str(i+1)))
assert writer.end_tx(), 'create edges tx'

threads = []
read_loops = 3000
write_loops = 1000
for i in range(num_readers):
    t = threading.Thread(target=read_loop, args=(readers[i], read_loops))
    t.start()
    threads.append(t)

cur_mod = 1
for i in range(write_loops):
    new_mod = random.randrange(1,num_nodes/2)
    remod_ring(writer, edges, cur_mod, new_mod)
    cur_mod = new_mod

for t in threads:
    t.join()

print 'Pass transactions_ring.'
