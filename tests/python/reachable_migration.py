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
import threading

import weaver.client as client

num_migr = 5
num_started = 0
num_finished = 0
num_clients = 1
cv = threading.Condition()

def exec_traversals(reqs, cl, exec_time, idx):
    global num_started
    global cv
    global num_clients
    global num_finished
    with cv:
        while num_started < num_clients:
            cv.wait()
    #rp = client.ReachParams()
    rp = client.ReachParams(edge_props=[('color','red')])
    start = time.time()
    cnt = 0
    for r in reqs:
        cnt += 1
        rp.dest = r[1]
        prog_args = [(r[0], rp)]
        response = cl.run_reach_program(prog_args)
        if cnt % 1 == 0:
            print 'done ' + str(cnt) + ' by client ' + str(idx)
    end = time.time()
    with cv:
        num_finished += 1
        cv.notify_all()
    exec_time[idx] = end - start

num_requests = 50
num_nodes = 81306 # snap twitter-combined
# node handles are range(0, num_nodes)
num_vts = 1

clients = []
for i in range(num_clients):
    clients.append(client.Client(client._CLIENT_ID + i, i % num_vts))

reqs = []
random.seed(42)
for i in range(num_clients):
    cl_reqs = []
    for numr in range(num_requests):
        cl_reqs.append((random.randint(0, num_nodes-1), random.randint(0, num_nodes-1)))
    reqs.append(cl_reqs)

# run before
exec_time = [0] * num_clients
threads = []
print 'Starting first set of requests'
for i in range(num_clients):
    thr = threading.Thread(target=exec_traversals, args=(reqs[i], clients[i], exec_time, i))
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
migr_begin = time.time()
for mrun in range(1,num_migr+1):
    clients[0].single_stream_migration()
    print 'Done repartitioning stream ' + str(mrun)
migr_end = time.time()
migr_time = migr_end - migr_begin
print 'Time for repartitioning: ' + str(migr_time)

# run after
exec_time = [0] * num_clients
threads = []
num_started = 0
num_finished = 0
print 'Starting second set of requests'
for i in range(num_clients):
    thr = threading.Thread(target=exec_traversals, args=(reqs[i], clients[i], exec_time, i))
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
