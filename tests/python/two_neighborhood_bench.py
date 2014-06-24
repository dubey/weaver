#! /usr/bin/env python
# 
# ===============================================================
#    Description:  Two neighborhood benchmark
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

import weaver.client as client
import simple_client

random.seed(42)
num_edges = 1768149
edge_sources = [None] * num_edges

def choose_random_pair():
    global edge_sources
    return (edge_sources[random.randint(0, num_edges-1)], edge_sources[random.randint(0, num_edges-1)])

if (len(sys.argv) != 2):
    print "want single extra arg for file to open"
    assert(False)

f = open(sys.argv[1])

i = 0
for line in f:
    if (line[0] is '#'):
        continue
    edge_sources[i] = int(line.split(" ")[0])
    i += 1
print "done loading file"

num_started = 0
num_finished = 0
cv = threading.Condition()


num_nodes = 81306 # snap twitter-combined
read_percent = 95
# node handles are range(0, num_nodes)
num_vts = 1
num_clients = 100
requests_per_client = 200

def add_labels(c, idx):
    global num_nodes
    tx_id = c.begin_tx()
    for i in range(num_nodes):
        if i % num_clients is idx:
            c.set_node_property(tx_id, i, 'name', str(i))
    assert(c.end_tx(tx_id))
    print "writing labels finished for client " + str(idx)

def exec_reads(reqs, sc, c, exec_time, idx):
    global num_started
    global cv
    global num_clients
    global num_finished
    with cv:
        while num_started < num_clients:
            cv.wait()
    start = time.time()
    cnt = 0
    for pair in reqs:
        cnt += 1
        if (random.randint(1,100) > read_percent) :
            tx_id = c.begin_tx()
            c.create_edge(tx_id, pair[0], pair[1])
            assert(c.end_tx(tx_id))
        else:
            two_neighborhood = sc.two_neighborhood(pair[0], "name", caching = True)
    end = time.time()
    with cv:
        num_finished += 1
        cv.notify_all()
    exec_time[idx] = end - start

clients = []
simple_clients = []
for i in range(num_clients):
    clients.append(client.Client(client._CLIENT_ID + i, i % num_vts))
    simple_clients.append(simple_client.simple_client(clients[i]))

reqs = []
for i in range(num_clients):
    cl_reqs = []
    for _ in range(requests_per_client):
        cl_reqs.append(choose_random_pair())
    reqs.append(cl_reqs)

exec_time = [0] * num_clients
threads = []
print "starting writes"
for i in range(num_clients):
    thr = threading.Thread(target=add_labels, args=(clients[i], i))
    thr.start()
    threads.append(thr)
for thr in threads:
    thr.join()

print "starting requests"
for i in range(num_clients):
    thr = threading.Thread(target=exec_reads, args=(reqs[i], simple_clients[i], clients[i], exec_time, i))
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
print 'Total time for ' + str(num_clients * requests_per_client) + 'requests = ' + str(total_time)
throughput = (num_clients * requests_per_client) / total_time
print 'Throughput = ' + str(throughput)
