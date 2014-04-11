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
sys.path.append('../../.libs')

import libclient as client

num_migr = 5
num_started = 0
num_finished = 0
num_clients = 300
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
        if cnt % 1000 == 0 and idx == 1:
            print 'done ' + str(cnt) + ' by client ' + str(idx)
    end = time.time()
    with cv:
        num_finished += 1
        cv.notify_all()
    exec_time[idx] = end - start

num_requests = 2000
num_nodes = 81306 # snap twitter-combined
# node handles are range(0, num_nodes)
num_vts = 1

clients = []
for i in range(num_clients):
    clients.append(client.Client(client._CLIENT_ID + i, i % num_vts))

reqs = []
for i in range(num_clients):
    cl_reqs = []
    for numr in range(num_requests):
        cl_reqs.append(random.randint(0, num_nodes-1))
    reqs.append(cl_reqs)

# run before
exec_time = [0] * num_clients
threads = []
print 'Starting first set of requests'
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
print 'Done first set of requests'

# repartition
for mrun in range(1,num_migr+1):
    clients[0].single_stream_migration()
    print 'Done repartitioning stream ' + str(mrun)

# run after
exec_time = [0] * num_clients
threads = []
num_started = 0
num_finished = 0
print 'Starting second set of requests'
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
print 'Done second set of requests'
