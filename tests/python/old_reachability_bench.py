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
sys.path.append('../../.libs')

import libclient as client

num_dests = 1000
requests_per_dest = 1

def exec_traversals(reqs, cl):
    rp = client.ReachParams(caching=True)
    start = time.time()
    for r in reqs:
        s= time.time()
        rp.dest = r[1]
        prog_args = [(r[0], rp)]
        response = cl.run_reach_program(prog_args)
        print '{0:05}'.format(r[0]) + " " + '{0:05}'.format(r[1]) + " " + str(time.time()-s)
        sys.stdout.flush()
    print ' done'
    end = time.time()
    return (end-start)

#num_nodes = 82168 # snap soc-Slashdot0902
#num_nodes = 10876 # snap p2pgnutella04
num_nodes = 81306 # snap twitter-combined
# node handles are range(0, num_nodes)

coord_id = 0
c = client.Client(client._CLIENT_ID, coord_id)

reqs = []
random.seed(42)
for _ in range(num_dests):
    dest = random.randint(0, num_nodes-1)
    for _ in range(requests_per_dest):
        reqs.append((random.randint(0, num_nodes-1), dest))

print "starting traversals"
t = exec_traversals(reqs, c)
print "time taken for " + str(num_dests * requests_per_dest) + " random reachability requests over " + str(num_nodes) + " nodes was: " + str(t)
