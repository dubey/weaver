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
import threading

import weaver.client as client

num_started = 0
num_finished = 0
num_clients = 100
cv = threading.Condition()

def exec_clusterings(reqs, cl, exec_time, idx):
    global num_started
    global cv
    global num_clients
    global num_finished
    with cv:
        while num_started < num_clients:
            cv.wait()
    cp = client.ClusteringParams()
    start = time.time()
    cnt = 0
    for r in reqs:
        cnt += 1
        prog_args = [(r, cp)]
        response = cl.run_clustering_program(prog_args)
        if cnt % 1000 == 0:
            print 'done ' + str(cnt) + ' by client ' + str(idx)
    end = time.time()
    with cv:
        num_finished += 1
        cv.notify_all()
    exec_time[idx] = end - start

num_requests = 2000
#num_nodes = 82168 # snap soc-Slashdot0902
#num_nodes = 10876 # snap p2pgnutella04
num_nodes = 81306 # snap twitter-combined
#num_nodes = 107600 # snap gplus-combined
#num_nodes = 2000
# node handles are range(0, num_nodes)
num_vts = 2

clients = []
for i in range(num_clients):
    clients.append(client.Client(client._CLIENT_ID + i, i % num_vts))

reqs = []
for i in range(num_clients):
    cl_reqs = []
    for numr in range(num_requests):
        cl_reqs.append(random.randint(0, num_nodes-1))
    reqs.append(cl_reqs)

exec_time = [0] * num_clients
threads = []
print "starting requests"
for i in range(num_clients):
    thr = threading.Thread(target=exec_clusterings, args=(reqs[i], clients[i], exec_time, i))
    thr.start()
    threads.append(thr)
start_time = time.time()
with cv:
    num_started = num_clients
    cv.notify_all()
    while num_finished < num_clients:
        cv.wait()
end_time = time.time()
total_time = end_time - start_time
for thr in threads:
    thr.join()
print 'Total time = ' + str(total_time)
throughput = (num_requests * num_clients) / total_time
print 'Throughput = ' + str(throughput)
