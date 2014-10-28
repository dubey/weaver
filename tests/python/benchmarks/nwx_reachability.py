# ===============================================================
#    Description:  Test performance of reachability program.
# 
#        Created:  12/17/2013 07:17:19 PM
# 
#         Author:  Greg Hill, gdh39@cornell.edu
# 
# Copyright (C) 2013, Cornell University, see the LICENSE file
#                     for licensing agreement
# ===============================================================
# 

import random
import sys
import time

import weaver.client as client
import simple_client
import test_base
import random
from networkx import *

num_nodes = 5000
num_dests = 200
requests_per_dest = 1

def exec_traversals(reqs, sc, caching):
    start = time.time()
    for r in reqs:
        assert(r[0] is not 0)
        assert(r[1] is not 0)
        sc.reachability(r[0], r[1], caching = caching)
        sys.stdout.write('.')
        sys.stdout.flush()
    print ' done'
    end = time.time()
    return (end-start)


coord_id = 0
c_list = []
c_list.append(client.Client(client._CLIENT_ID, coord_id))
c_list.append(client.Client(client._CLIENT_ID + 1, coord_id))
c_list.append(client.Client(client._CLIENT_ID + 2, coord_id))
sc = simple_client.simple_client(c_list[0])

reqs = []
random.seed(42)
g = test_base.test_graph(c_list, num_nodes, 2*num_nodes, seed = 42)

for _ in range(num_dests):
    dest = random.choice(g.nodes)
    for _ in range(requests_per_dest):
        reqs.append((random.choice(g.nodes), dest))

print "starting traversals"

t = exec_traversals(reqs, sc, False)
print "time taken for " + str(num_dests * requests_per_dest) + " random reachability requests over " + str(num_nodes) + " nodes was: " + str(t)

t = exec_traversals(reqs, sc, True)
print "time taken for " + str(num_dests * requests_per_dest) + " random reachability requests with hot caching over " + str(num_nodes) + " nodes was: " + str(t)
