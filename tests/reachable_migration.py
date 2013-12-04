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

num_requests = 1000
#num_nodes = 82168 # snap soc-Slashdot0902
num_nodes = 10876 # snap p2pgnutella04
# node handles are range(0, num_nodes)

coord_id = 0
c = client.Client(client._CLIENT_ID + 1, coord_id)

# enable dynamic repartitioning
#c.start_migration()

rp = client.ReachParams()
start = time.time()
for i in range(num_requests):
    src = random.randint(0, num_nodes-1)
    dst = random.randint(0, num_nodes-1)
    rp.dest = dst
    prog_args = [(src, rp)]
    response = c.run_reach_program(prog_args)
    if i % 10 == 0:
        print 'Completed ' + str(i) + ' requests'
    if i == num_requests/2:
        end = time.time()
        first = end - start
        for mrun in range(1,6):
            c.single_stream_migration()
            print 'Done repartitioning stream ' + str(mrun)
        start = time.time()
end = time.time()
second = end - start
print 'Throughput before partitioning = ' + str(num_requests/(2*first))
print 'Throughput after partitioning = ' + str(num_requests/(2*second))
