# 
# ===============================================================
#    Description:  Test performance of dynamic repartitioning on
#                  clustering program.
# 
#        Created:  12/08/2013 01:09:08 PM
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

def exec_clusterings(reqs, cl):
    cp = client.ClusteringParams()
    start = time.time()
    cnt = 0
    for r in reqs:
        cnt += 1
        prog_args = [(r[0], cp)]
        response = cl.run_clustering_program(prog_args)
        print 'Got clustering response ' + str(response.clustering_coeff)
        #if cnt % 1 == 0:
        #    sys.stdout.write('.')
        #    sys.stdout.flush()
    print ' done'
    end = time.time()
    return (end-start)

num_requests = 20
#num_nodes = 82168 # snap soc-Slashdot0902
#num_nodes = 10876 # snap p2pgnutella04
num_nodes = 81306 # snap twitter-combined
# node handles are range(0, num_nodes)

coord_id = 0
c = client.Client(client._CLIENT_ID, coord_id)

reqs = []
random.seed(42)
for numr in range(num_requests):
    reqs.append(random.randint(0, num_nodes-1))

print 'Before streaming rounds, time taken: ' + str(exec_traversals(reqs, c))
print 'Before streaming rounds, time taken: ' + str(exec_traversals(reqs, c))
for mrun in range(1,6):
    c.single_stream_migration()
    print 'Done repartitioning stream ' + str(mrun)
print 'After streaming rounds, time taken: ' + str(exec_traversals(reqs, c))
print 'After streaming rounds, time taken: ' + str(exec_traversals(reqs, c))
