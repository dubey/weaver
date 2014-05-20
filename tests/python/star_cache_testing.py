import sys
sys.path.append('../../.libs')

import libclient as client
import time

# creating star graph, tests one source many destinations with disjoint paths (aka max cache size)
nodes = []
num_nodes = 2000
coord_id = 0
c = client.Client(client._CLIENT_ID, coord_id)

tx_id = c.begin_tx()
center = c.create_node(tx_id)
for i in range(num_nodes):
    nodes.append(c.create_node(tx_id))
    #print 'Created node ' + str(i)
c.end_tx(tx_id)
tx_id = c.begin_tx()
for i in range(num_nodes):
    c.create_edge(tx_id, center, nodes[i])
    #print 'Created edge ' + str(i)
c.end_tx(tx_id)
print 'Created graph'

start = time.time()
print('Running rechability to node: '),
for i in range(num_nodes):
    rp = client.ReachParams(dest=nodes[i], caching=False)
    prog_args = [(center, rp)]
    response = c.run_reach_program(prog_args)
    print(str(i) + ','),
    sys.stdout.flush()
    assert(response.reachable)
for i in range(num_nodes):
    rp = client.ReachParams(dest=nodes[i], caching=True)
    prog_args = [(center, rp)]
    response = c.run_reach_program(prog_args)
    print(str(i) + ','),
    sys.stdout.flush()
    assert(response.reachable)
print 'successful'
print 'Ran reachability in ' + str(time.time()-start) + ' seconds'
