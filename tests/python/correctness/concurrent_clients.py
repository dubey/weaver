#! /usr/bin/env python
# 
# ===============================================================
#    Description:  Lots of clients doing concurrent requests 
#                  Warning: may require raising ulimit -n
# 
#         Author:  Ayush Dubey, dubey@cs.cornell.edu
# 
# Copyright (C) 2015, Cornell University, see the LICENSE file
#                     for licensing agreement
# ===============================================================
# 

import sys
import threading

try:
    from weaver import client
except ImportError:
    import client

config_file=''

if len(sys.argv) > 1:
    config_file = sys.argv[1]


# exec lots of get node requests
def flood_get_node(c, num_reqs, idx):
    for i in range(num_reqs):
        n = c.get_node('ayush')
        assert n.properties['type'] == ['user']


num_clients = 500
num_requests = 100
clients = []

# create clients
for i in range(num_clients):
    clients.append(client.Client('127.0.0.1', 2002, config_file))

print 'created clients'

# create node for user ayush
clients[0].begin_tx()
clients[0].create_node('ayush')
clients[0].set_node_properties({'type': 'user', 'age': '25'}, 'ayush')
clients[0].end_tx()

print 'created node ayush'

threads = []
for i in range(num_clients):
    t = threading.Thread(target=flood_get_node, args=(clients[i], num_requests, i))
    t.start()
    threads.append(t)

print 'started threads'

for i in range(num_clients):
    threads[i].join()

print 'done all threads'

clients[0].begin_tx()
clients[0].delete_node('ayush')
clients[0].end_tx()

print 'Concurrent clients successful, executed ' + str(num_clients*num_requests) + ' requests'
