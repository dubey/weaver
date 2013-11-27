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
sys.path.append('../bindings/python')

import client
print ' run reachability on'
print ' paralell lines from source to sink'
print ' part way through cut links'
print '     ______cut1______________________'
print '    |                                |'
print '    |___________cut2_________________|'
print ' X--|                                |--X'
print '    |________________cut3____________|'
print '    |                                |'
print '    |_____________________cut4_______|'
print ''

# creating line graph
nodes_per_line = 1000
line1 = []
line2 = []
line3 = []
line4 = []
coord_id = 0
c = client.Client(client._CLIENT_ID+1, coord_id)

tx_id = c.begin_tx()
source = c.create_node(tx_id);
sink = c.create_node(tx_id);

print 'Creating nodes'
for i in range(nodes_per_line):
    line1.append(c.create_node(tx_id))
    line2.append(c.create_node(tx_id))
    line3.append(c.create_node(tx_id))
    line4.append(c.create_node(tx_id))

print 'Created nodes, creating edges'
c.create_edge(tx_id, source, line1[0])
c.create_edge(tx_id, source, line2[0])
c.create_edge(tx_id, source, line3[0])
c.create_edge(tx_id, source, line4[0])

print 'Connected source to lines'
c.create_edge(tx_id, line1[nodes_per_line-1], sink)
c.create_edge(tx_id, line2[nodes_per_line-1], sink)
c.create_edge(tx_id, line3[nodes_per_line-1], sink)
c.create_edge(tx_id, line4[nodes_per_line-1], sink)
print 'Connected lines to sink'

for i in range(nodes_per_line-1):
    if i == 1*nodes_per_line/5:
        cut1 = c.create_edge(tx_id, line1[i], line1[i+1])
    else:
        c.create_edge(tx_id, line1[i], line1[i+1])

    if i == 2*nodes_per_line/5:
        cut2 = c.create_edge(tx_id, line2[i], line2[i+1])
    else:
        c.create_edge(tx_id, line2[i], line2[i+1])

    if i == 3*nodes_per_line/5:
        cut3 = c.create_edge(tx_id, line3[i], line3[i+1])
    else:
        c.create_edge(tx_id, line3[i], line3[i+1])

    if i == 4*nodes_per_line/5:
        cut4 = c.create_edge(tx_id, line4[i], line4[i+1])
    else:
        c.create_edge(tx_id, line4[i], line4[i+1])

print 'Connected all lines'
c.end_tx(tx_id)

print 'Created graph'

rp = client.ReachParams(dest=sink, caching=True)
print 'Created reach param: mode = ' + str(rp.mode) + ', reachable = ' + str(rp.reachable)
prog_args = [(source, rp)]
response = c.run_reach_program(prog_args)
assert(response.reachable)
print 'Sink reachable from source before cuts'
tx_id = c.begin_tx()
c.delete_edge(tx_id, line1[1*nodes_per_line/5], cut1)
c.end_tx(tx_id)
prog_args = [(source, rp)]
response = c.run_reach_program(prog_args)
assert(response.reachable)
print 'Sink reachable from source after cut1'
tx_id = c.begin_tx()
c.delete_edge(tx_id, line2[2*nodes_per_line/5], cut2)
c.end_tx(tx_id)
prog_args = [(source, rp)]
response = c.run_reach_program(prog_args)
assert(response.reachable)
print 'Sink reachable from source after cut2'
tx_id = c.begin_tx()
c.delete_edge(tx_id, line3[3*nodes_per_line/5], cut3)
c.end_tx(tx_id)
prog_args = [(source, rp)]
response = c.run_reach_program(prog_args)
assert(response.reachable)
print 'Sink reachable from source after cut3'
tx_id = c.begin_tx()
c.delete_edge(tx_id, line4[4*nodes_per_line/5], cut4)
c.end_tx(tx_id)
prog_args = [(source, rp)]
response = c.run_reach_program(prog_args)
assert(not response.reachable)
print 'Sink not reachable from source after cut4'
print ''
print 'Running reachability tests on line 1'
print('Not reachable from node'),
for i in range(nodes_per_line):
    if i == 1*nodes_per_line/5 + 1:
        print 'to sink'
        print 'past cut1'
        print('Reachable from node'),

    prog_args = [(line1[i], rp)]
    response = c.run_reach_program(prog_args)
    print(str(i) + ','),
    sys.stdout.flush()
    assert(response.reachable is (i > 1*nodes_per_line/5))
print 'to sink'
print ''
print 'Running reachability tests on line 2'
print('Not reachable from node'),
for i in range(nodes_per_line):
    if i == 2*nodes_per_line/5 + 1:
        print 'to sink'
        print 'past cut2'
        print('Reachable from node'),

    prog_args = [(line2[i], rp)]
    response = c.run_reach_program(prog_args)
    print(str(i) + ','),
    sys.stdout.flush()
    assert(response.reachable is (i > 2*nodes_per_line/5))
print 'to sink'
print ''
print 'Running reachability tests on line 3'
print('Not reachable from node'),
for i in range(nodes_per_line):
    if i == 3*nodes_per_line/5 + 1:
        print 'to sink'
        print 'past cut3'
        print('Reachable from node'),

    prog_args = [(line3[i], rp)]
    response = c.run_reach_program(prog_args)
    print(str(i) + ','),
    sys.stdout.flush()
    assert(response.reachable is (i > 3*nodes_per_line/5))
print 'to sink'
print ''
print 'Running reachability tests on line 4'
print('Not reachable from node'),
for i in range(nodes_per_line):
    if i == 4*nodes_per_line/5 + 1:
        print 'to sink'
        print 'past cut4'
        print('Reachable from node'),

    prog_args = [(line4[i], rp)]
    response = c.run_reach_program(prog_args)
    print(str(i) + ','),
    sys.stdout.flush()
    assert(response.reachable is (i > 4*nodes_per_line/5))
print 'to sink'
print 'All tests passed'
