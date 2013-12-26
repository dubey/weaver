# 
# ===============================================================
#    Description:  Test performance of dynamic repartitioning on
#                  reachability program.
# 
#        Created:  11/25/2013 07:17:19 PM
# 
#         Author:  Ayush Dubey, dubey@cs.cornell.edu
# 
# Copyright (C) 2013, Cornell University, see the LICENSE file
#                     for licensing agreement
# ===============================================================
# 

import random
import sys
import time
sys.path.append('../bindings/python')

import client

def exec_traversals(reqs, cl):
    rp = client.ReachParams()
    #rp = client.ReachParams(edge_props=[('color','blue')])
    start = time.time()
    cnt = 0
    for r in reqs:
        cnt += 1
        rp.dest = r[1]
        prog_args = [(r[0], rp)]
        response = cl.run_reach_program(prog_args)
        if cnt % 1 == 0:
            sys.stdout.write('.')
            sys.stdout.flush()
    print ' done'
    end = time.time()
    return (end-start)

num_requests = 100
num_runs = 4
#num_nodes = 82168 # snap soc-Slashdot0902
#num_nodes = 10876 # snap p2pgnutella04
#num_nodes = 81306 # snap twitter-combined
num_nodes = 325729 # snap web-NotreDame
# node handles are range(0, num_nodes)

coord_id = 0
c = client.Client(client._CLIENT_ID, coord_id)

reqs = []
random.seed(100)
for numr in range(num_requests):
    reqs.append((random.randint(0, num_nodes-1), random.randint(0, num_nodes-1)))

t1 = 0
for run in range(num_runs):
    t1 += exec_traversals(reqs, c)
print 'Before streaming rounds, time taken: ' + str(t1)
c.print_msgcount()
tpart = time.time()
for mrun in range(1,3):
    c.single_stream_migration()
    print 'Done repartitioning stream ' + str(mrun)
tpart = time.time() - tpart
print 'Repartitioning time = ' + str(tpart)
t2 = 0
for run in range(num_runs):
    t2 += exec_traversals(reqs, c)
print 'After streaming rounds, time taken: ' + str(t2)
c.print_msgcount()
