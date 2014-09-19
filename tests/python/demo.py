#! /usr/bin/env python
# 
# ===============================================================
#    Description:  Demonstrate basic Weaver API. 
# 
#        Created:  2014-06-24 09:12:15
# 
#         Author:  Ayush Dubey, dubey@cs.cornell.edu
# 
# Copyright (C) 2013, Cornell University, see the LICENSE file
#                     for licensing agreement
# ===============================================================
# 

import weaver.client as client

# create client object
c = client.Client('127.0.0.1', 2002)
print 'created client'

# create node for user ayush
c.begin_tx()
c.create_node('ayush')
c.set_node_property('ayush', 'type', 'user')
success = c.end_tx()
if success:
    print 'User ayush created'
else:
    print 'tx fail'
#raw_input("Press enter.")

# create node for user egs
c.begin_tx()
c.create_node('egs')
c.set_node_property('egs', 'type', 'user')
success = c.end_tx()
if success:
    print 'User egs created'
else:
    print 'tx fail'
#raw_input("Press enter.")

# ayush follows egs
c.begin_tx()
c.create_edge('ayush', 'egs', 'e1')
c.set_edge_property('ayush', 'e1', 'type', 'follows')
e2 = c.create_edge('egs', 'ayush', 'e2')
print 'e2: ' + e2
c.set_edge_property('egs', e2, 'type', 'followed_by')
success = c.end_tx()
if success:
    print 'ayush follows egs'
else:
    print 'tx fail'
#raw_input("Press enter.")

# add a post and restrict visibility to followers only
c.begin_tx()
c.create_node('post')
c.set_node_property('post', 'type', 'post')
c.set_node_property('post', 'visibility', 'followers')
e3 = c.create_edge('egs', 'post')
#assert e3 == 'e3'
print 'e3: ' + e3
c.set_edge_property('egs', e3, 'type', 'posted')
success = c.end_tx()
if success:
    print 'egs posted content'
else:
    print 'tx fail'

# 'like' the post
c.begin_tx()
e4 = c.create_edge('post', 'ayush')
c.set_edge_property('post', e4, 'type', 'liked_by')
success = c.end_tx()
if success:
    print 'ayush likes egs\'s post'
else:
    print 'tx fail'

# list all the people who like egs's post
return_nodes = c.traverse('egs', [('type','user')]).out_edge([('type','posted')]).node([('type','post')]).out_edge([('type','liked_by')]).node([('type','user')]).execute()
print 'List of users who like egs\'s posts:'
for user in return_nodes:
    print '\t' + user


# all 2-hop nodes and edges
two_hop = c.traverse('egs').out_edge().node().out_edge().node().collect()
print 'List of 2-hop nodes and edges from egs'
for elem in two_hop:
    print '\t' + elem

# 2-hop nodes
two_hop_nodes = c.traverse('egs').out_edge().node().out_edge().node().collect_nodes()
print 'List of 2-hop nodes from egs'
for elem in two_hop_nodes:
    print '\t' + elem


c.begin_tx()
print 'default created node: ' + c.create_node()
print 'default created node: ' + c.create_node()
print 'default created node: ' + c.create_node()
print 'default created node: ' + c.create_node()
c.end_tx()

# create node for user egs
c.begin_tx()
c.create_node('egs')
c.set_node_property('egs', 'type', 'user')
success = c.end_tx()
if success:
    print 'User egs created'
else:
    print 'tx fail'
