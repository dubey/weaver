#! /usr/bin/env python
# 
# ===============================================================
#    Description:  Sanity check for fresh install. 
# 
#        Created:  2014-08-12 16:42:52
# 
#         Author:  Ayush Dubey, dubey@cs.cornell.edu
# 
# Copyright (C) 2013, Cornell University, see the LICENSE file
#                     for licensing agreement
# ===============================================================
# 

import sys

try:
    from weaver import client
except ImportError:
    import client

config_file=''

if len(sys.argv) > 1:
    config_file = sys.argv[1]

# create client object
c = client.Client('127.0.0.1', 2002, config_file)

# create node for user ayush
c.begin_tx()
c.create_node('ayush')
c.set_node_property('type', 'user', 'ayush')
c.end_tx()

# create node for user egs
c.begin_tx()
c.create_node('egs')
c.set_node_property('type', 'user', 'egs')
c.end_tx()

# ayush follows egs
c.begin_tx()
c.create_edge('ayush', 'egs', 'e1')
c.set_edge_property('e1', 'type', 'follows', 'ayush')
c.create_edge('egs', 'ayush', 'e2')
c.set_edge_property('e2', 'type', 'followed_by', 'egs')
c.end_tx()

# add a post and restrict visibility to followers only
c.begin_tx()
c.create_node('post')
c.set_node_property('type', 'post', 'post')
c.set_node_property('visibility', 'followers', 'post')
e3 = c.create_edge('egs', 'post')
c.set_edge_property(e3, 'type', 'posted', 'egs')
c.end_tx()

# 'like' the post
c.begin_tx()
e4 = c.create_edge('post', 'ayush')
c.set_edge_property(e4, 'type', 'liked_by', 'post')
c.end_tx()

# list all the people who like egs's post
return_nodes = c.traverse('egs', {'type': 'user'}).out_edge({'type': 'posted'}).node({'type': 'post'}).out_edge({'type': 'liked_by'}).node({'type': 'user'}).execute()
assert len(return_nodes) == 1, 'traversal returned incorrect #nodes'
assert 'ayush' in return_nodes, 'traversal returned bad node handle'

# try to create node with same handle as before
c.begin_tx()
c.create_node('ayush')
try:
    c.end_tx()
    assert False, 'create node passed'
except client.WeaverError:
    pass

# try to create edge with same handle as before
c.begin_tx()
c.create_edge('ayush', 'egs', 'e1')
try:
    c.end_tx()
    assert False, 'create edge passed'
except client.WeaverError:
    pass

print 'Correctly executed 8 transactions of varying complexity, pass simple_test.'
print 'Success, you have a working Weaver setup!'
