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

import weaver.client as client

num_started = 0
num_finished = 0
num_clients = 40
cv = threading.Condition()
all_latencies = []

class request_gen:
    def __init__(self):
        # node handles are range(0, num_nodes)
        self.num_nodes = 10000 # snap livejournal 10k sample
        #self.num_nodes = 81306 # snap twitter-combined
        #self.num_nodes = 4840000 # snap livejournal

        self.p_read = 1
        self.p_assoc_get = 0.157
        self.p_assoc_range = 0.437
        self.p_assoc_count = 0.117
        self.p_obj_get = 0.289
        self.c_assoc_get = self.p_assoc_get
        self.c_assoc_range = self.c_assoc_get + self.p_assoc_range
        self.c_assoc_count = self.c_assoc_range + self.p_assoc_count
        self.c_obj_get = self.c_assoc_count + self.p_obj_get

        self.p_write = 0.002
        self.p_assoc_add = 0.8
        self.p_assoc_del = 0.2
        self.p_assoc_update = 0.207
        #self.p_assoc_add = 0.525
        #self.p_assoc_del = 0.083
        #self.p_obj_add = 0.165
        #self.p_obj_update = 0.207
        #self.p_obj_del = 0.02
        self.c_assoc_add = self.p_assoc_add
        self.c_assoc_del = self.c_assoc_add + self.p_assoc_del
        #self.c_assoc_add = self.p_assoc_add
        #self.c_assoc_del = self.c_assoc_add + self.p_assoc_del
        #self.c_obj_add = self.c_assoc_del + self.p_obj_add
        #self.c_obj_update = self.c_obj_add + self.p_obj_update
        #self.c_obj_del = self.c_obj_update + self.p_obj_del

        self.req_types = ['assoc_get', 'assoc_range', 'assoc_count', 'obj_get',
                          'assoc_add', 'assoc_del', 'obj_add', 'obj_update', 'obj_del']


    def get(self):
        coin_toss = random.random()
        n1 = str(random.randint(0, self.num_nodes-1))
        n2 = str(random.randint(0, self.num_nodes-1))
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
            #return [4, n1, n2]
            coin_toss = random.random()
            if coin_toss < self.c_assoc_add:
                return [4, n1, n2]
            else:
                return [5, n1, n2]
            #elif coin_toss < self.c_obj_add:
            #    return [6]
            #elif coin_toss < self.c_obj_update:
            #    return [7, n1]
            #else:
            #    return [8, n1]


def exec_work(idx, cl, num_requests):
    global num_started
    global num_finished
    global num_clients
    global cv
    global all_latencies
    assert(num_requests % 1000 == 0)
    rgen = request_gen()
    egp = client.EdgeGetParams()
    ecp = client.EdgeCountParams()
    rnep = client.ReadNEdgesParams()
    rnpp = client.ReadNodePropsParams()

    with cv:
        while num_started < num_clients:
            cv.wait()

    edge_list = []
    edge_parent_list = []
    latencies = []
    for rcnt in range(num_requests):
        req = rgen.get()
        start = time.time();
        if req[0] == 0:  # assoc_get
            response = cl.get_edges(node=req[1], nbrs=[req[2]])
        elif req[0] == 1: # assoc range
            rnep.num_edges = 50
            prog_args = [(req[1], rnep)]
            response = cl.read_n_edges(prog_args)
        elif req[0] == 2: # assoc count
            prog_args = [(req[1], ecp)]
            response = cl.edge_count(prog_args)
        elif req[0] == 3: # obj get
            response = cl.get_node(req[1], get_props=True)
        elif req[0] == 4: # assoc_add
            cl.begin_tx()
            new_edge = cl.create_edge(req[1], req[2])
            cl.end_tx()
            print 'done tx 4'
            edge_list.append(new_edge)
            edge_parent_list.append(req[1])
        elif req[0] == 5: # assoc del
            if len(edge_list) > 0:
                cl.begin_tx()
                del_edge = edge_list.pop()
                del_edge_node = edge_parent_list.pop()
                cl.delete_edge(del_edge, del_edge_node)
                cl.end_tx()
            print 'done tx 5'
        #elif req[0] == 6: # assoc update
        #    tx_id = cl.begin_tx()
        #    if len(edge_list) > 0:
        #        rnd_idx = random.randint(0, len(edge_list)-1)
        #        edge = edge_list[rnd_idx]
        #        node = edge_parent_list[rnd_idx]
        #        cl.set_edge_property(tx_id, node, edge, 'color', 'red')
        #    cl.end_tx(tx_id)
        else:
            print 'BAD VALUE!'
        #elif req[0] == 4: # assoc add
        #    print 'write 4'
        #elif req[0] == 5: # assoc del
        #    print 'write 5'
        #elif req[0] == 6: # obj add
        #    print 'write 6'
        #elif req[0] == 7: # obj update
        #    print 'write 7'
        #elif req[0] == 8: # obj del
        #    print 'write 8'
        #else:
        #    print 'unknown request type'
        #    assert(False)
        end = time.time()
        latencies.append(end-start)
        if rcnt > 0 and rcnt % 100 == 0:
            print 'done ' + str(rcnt) + ' by client ' + str(idx)
    with cv:
        num_finished += 1
        cv.notify_all()
        all_latencies.extend(latencies)

def main():
    num_requests = 5000
    global num_started
    global num_finished
    global num_clients
    global cv
    global all_latencies

    clients = []
    for i in range(num_clients):
        clients.append(client.Client('128.84.167.101', 2002))

    threads = []
    print "starting requests"
    for i in range(num_clients):
        thr = threading.Thread(target=exec_work, args=(i, clients[i], num_requests))
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

    lat_file = open('latencies', 'w')
    for t in all_latencies:
        lat_file.write(str(t) + '\n')

if __name__ == '__main__':
    main()
