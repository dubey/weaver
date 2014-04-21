#! /usr/bin/env python
# 
# ===============================================================
#    Description:  Reachability benchmark
# 
#        Created:  2014-03-21 13:39:06
# 
#         Author:  Ayush Dubey, dubey@cs.cornell.edu
# 
# Copyright (C) 2013-2014, Cornell University, see the LICENSE
#                     file for licensing agreement
# ===============================================================
# 

import random
import sys
import time
import threading
sys.path.append('../../.libs')

import libclient as client
import simple_client


num_started = 0
num_finished = 0
cv = threading.Condition()

dests_per_client = 1
requests_per_dest = 20

num_nodes = 81306 # snap twitter-combined
# node handles are range(0, num_nodes)
num_vts = 1
num_clients = 25

def exec_reads(reqs, sc, exec_time, idx):
    global num_started
    global cv
    global num_clients
    global num_finished
    with cv:
        while num_started < num_clients:
            cv.wait()
    start = time.time()
    cnt = 0
    for (source, dest) in reqs:
        cnt += 1
        reachable = sc.reachability(source, dest, caching = True)[0]
        if (not reachable):
            print str(dest) + " not reachable from " + str(source)
    end = time.time()
    with cv:
        num_finished += 1
        cv.notify_all()
    exec_time[idx] = end - start

clients = []
for i in range(num_clients):
    clients.append(simple_client.simple_client(client.Client(client._CLIENT_ID + i, i % num_vts)))

reqs = []
random.seed(42)
for i in range(num_clients):
    cl_reqs = []
    for _ in range(dests_per_client):
        dest = random.randint(0, num_nodes-1)
        for _ in range(requests_per_dest):
            cl_reqs.append((random.randint(0, num_nodes-1), dest))

    reqs.append(cl_reqs)

exec_time = [0] * num_clients
threads = []
print "starting requests"
for i in range(num_clients):
    thr = threading.Thread(target=exec_reads, args=(reqs[i], clients[i], exec_time, i))
    thr.start()
    threads.append(thr)
start_time = time.time()
with cv:
    num_started = num_clients
    cv.notify_all()
    while num_finished < num_clients:
        cv.wait()
end_time = time.time()
total_time = end_time-start_time
for thr in threads:
    thr.join()
print 'Total time for ' + str(dests_per_client * requests_per_dest * num_clients) + 'requests = ' + str(total_time)
throughput = (dests_per_client * requests_per_dest * num_clients) / total_time
print 'Throughput = ' + str(throughput)
