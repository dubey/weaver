# 
# ===============================================================
#    Description:  Cache testing with Line reachability program in Python.
# 
#        Created:  11/11/2013 03:17:55 PM
# 
#         Author:  Ayush Dubey, dubey@cs.cornell.edu
# 
# Copyright (C) 2013, Cornell University, see the LICENSE file
#                     for licensing agreement
# ===============================================================
# 

import sys
sys.path.append('../../.libs')

import time
import libclient as client
import simple_client

# creating line graph
nodes = []
num_nodes = 400
coord_id = 0
c = client.Client(client._CLIENT_ID+1, coord_id)
sc = simple_client.simple_client(c)

tx_id = c.begin_tx()
for i in range(num_nodes):
    nodes.append(c.create_node(tx_id))
    #print 'Created node ' + str(i)
c.end_tx(tx_id)

tx_id = c.begin_tx()
for i in range(num_nodes-1):
    if i == num_nodes/2:
        break_edge = c.create_edge(tx_id, nodes[i], nodes[i+1])
    else:
        c.create_edge(tx_id, nodes[i], nodes[i+1])

    #print 'Created edge ' + str(i)
c.end_tx(tx_id)

print 'Created graph'

start_time = time.time()
reachable = sc.reachability(nodes[0], nodes[num_nodes-1], caching=True)[0]
#print 'From node ' + str(0) + ' to node ' + str(num_nodes-1) + ', reachable = ' + str(reachable)
assert(reachable)
for i in range(num_nodes):
    reachable = sc.reachability(nodes[i], nodes[num_nodes-1], caching=True)[0]
    #print 'From node ' + str(i) + ' to node ' + str(num_nodes-1) + ', reachable = ' + str(reachable)
    assert(reachable)

print 'deleting middle edge ' + str(break_edge) + ' and retry'
tx_id = c.begin_tx()
c.delete_edge(tx_id, break_edge, nodes[num_nodes/2])
c.end_tx(tx_id)

reachable = sc.reachability(nodes[0], nodes[num_nodes-1], caching=True)[0]
#print 'From node ' + str(0) + ' to node ' + str(num_nodes-1) + ', reachable = ' + str(reachable)
assert(not reachable)

for i in range(num_nodes):
    if i == num_nodes/2 + 1:
        print 'past broken chain point'

    reachable = sc.reachability(nodes[i], nodes[num_nodes-1], caching=True)[0]
    #print 'From node ' + str(i) + ' to node ' + str(num_nodes-1) + ', reachable = ' + str(reachable)
    assert(reachable is (i > num_nodes/2))

end_time = time.time()
print 'Total time for test was ' + str(end_time-start_time)

