#! /usr/bin/env python
# 
# ===============================================================
#    Description:  Benchmark to emulate TAO workload. 
# 
#        Created:  2014-04-17 10:47:00
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

num_started = 0
num_finished = 0
num_clients = 100
cv = threading.Condition()

class request_gen:
    def __init__():
        # node handles are range(0, num_nodes)
        self.num_nodes = 81306 # snap twitter-combined

        self.p_read = 0.998
        self.p_assoc_get = 0.157
        self.p_assoc_range = 0.437
        self.p_assoc_count = 0.117
        self.p_obj_get = 0.289
        self.c_assoc_get = self.p_assoc_get
        self.c_assoc_range = self.c_assoc_get + self.p_assoc_range
        self.c_assoc_count = self.c_assoc_range + self.p_assoc_count
        self.c_obj_get = c_assoc_count + self.p_obj_get

        self.p_write = 0.2
        self.p_assoc_add = 0.525
        self.p_assoc_del = 0.083
        self.p_obj_add = 0.165
        self.p_obj_update = 0.207
        self.p_obj_del = 0.02
        self.c_assoc_add = self.p_assoc_add
        self.c_assoc_del = self.c_assoc_add + self.p_assoc_del
        self.c_obj_add = self.c_assoc_del + self.p_obj_add
        self.c_obj_update = self.c_obj_add + self.p_obj_update
        self.c_obj_del = self.c_obj_update + self.p_obj_del

        self.req_types = ['assoc_get', 'assoc_range', 'assoc_count', 'obj_get',
                          'assoc_add', 'assoc_del', 'obj_add', 'obj_update', 'obj_del']


    def get():
        coin_toss = random.random()
        n1 = random.randint(0, self.num_nodes-1)
        n2 = random.randint(0, self.num_nodes-1)
        if coin_toss < self.p_read:
            coin_toss = random.random()
            if coin_toss < self.c_assoc_get:
                return [0, n1, n2]
            elif coin_toss < self.c_assoc_range:
                return [1, n1]
            elif coin_toss < self.c_assoc_count:
                return [2, n1]
            else:
                return [3, n1]
        else:
            coin_toss = random.random()
            if coin_toss < self.c_assoc_add:
                return [4, n1, n2]
            elif coin_toss < self.c.assoc_del:
                return [5, n1, n2]
            elif coin_toss < self.c_obj_add:
                return [6]
            elif coin_toss < self.obj_update:
                return [7, n1]
            else:
                return [8, n1]


def exec_work(cl):
    global num_started
    global num_finished
    global num_clients
    global cv
    num_requests = 2000
    assert(num_requests % 1000 == 0)
    request_gen rgen
    egp = client.EdgeGetParams()
    ecp = client.EdgeCountParams()
    rnep = client.ReadNEdgesParams()
    rnpp = client.ReadNodePropsParams()

    with cv:
        while num_started < num_clients:
            cv.wait()

    for rcnt in range(num_requests):
        req = rgen.get()
        if req[0] == 0:  # assoc_get
            egp.nbr_id = req[2]
            prog_args = [(req[1], egp)]
            response = cl.edge_get(prog_args)
        elif req[0] == 1: # assoc range
            rnep.num_edges = 50
            prog_args = [(req[1], rnep)]
            response = cl.read_n_edges(prog_args)
        elif req[0] == 2: # assoc count
            prog_args = [(req[1], ecp)]
            response = cl.edge_count(prog_args)
        elif req[0] == 3: # obj get
            prog_args = [(req[1], rnpp)]
            response = cl.read_node_props(prog_args)
        elif req[0] == 4: # assoc add
            print 'write 4'
        elif req[0] == 5: # assoc del
            print 'write 5'
        elif req[0] == 6: # obj add
            print 'write 6'
        elif req[0] == 7: # obj update
            print 'write 7'
        elif req[0] == 8: # obj del
            print 'write 8'
        else:
            print 'unknown request type'
            assert(False)
        if rcnt % 1000 == 0:
            print 'done ' + str(rcnt) + ' by client ' + str(idx)
    with cv:
        num_finished += 1
        cv.notify_all()

num_vts = 1

clients = []
for i in range(num_clients):
    clients.append(client.Client(client._CLIENT_ID + i, i % num_vts))

threads = []
print "starting requests"
for i in range(num_clients):
    thr = threading.Thread(target=exec_work, args=(clients[i]))
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
print 'Total time = ' + str(total_time)
throughput = (num_requests * num_clients) / total_time
print 'Throughput = ' + str(throughput)
for thr in threads:
    thr.join()
