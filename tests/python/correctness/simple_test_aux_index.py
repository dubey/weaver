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
    import weaver.client as client
except ImportError:
    import client

config_file=''

if len(sys.argv) > 1:
    config_file = sys.argv[1]

# create client object
c = client.Client('127.0.0.1', 2002, config_file)

# check aux index
assert c.aux_index()

# create node for user ayush
c.begin_tx()
c.create_node('ayush')
c.set_node_property('ayush', 'type', 'user')
assert c.end_tx(), 'create node failed'

# create node for user egs
c.begin_tx()
c.create_node('egs')
c.set_node_property('egs', 'type', 'user')
assert c.end_tx(), 'create node failed'

# ayush follows egs
c.begin_tx()
c.create_edge('ayush', 'egs', 'e1')
c.set_edge_property(edge='e1', key='type', value='follows')
c.create_edge('egs', 'ayush', 'e2')
c.set_edge_property(edge='e2', key='type', value='followed_by')
assert c.end_tx(), 'tx fail, something is wrong'

# add a post and restrict visibility to followers only
c.begin_tx()
c.create_node('post')
c.set_node_property('post', 'type', 'post')
c.set_node_property('post', 'visibility', 'followers')
e3 = c.create_edge('egs', 'post')
c.set_edge_property(edge=e3, key='type', value='posted')
assert c.end_tx(), 'tx fail, something is wrong'

# 'like' the post
c.begin_tx()
e4 = c.create_edge('post', 'ayush')
c.set_edge_property(edge=e4, key='type', value='liked_by')
assert c.end_tx(), 'create edge failed'

# list all the people who like egs's post
return_nodes = c.traverse('egs', [('type','user')]).out_edge([('type','posted')]).node([('type','post')]).out_edge([('type','liked_by')]).node([('type','user')]).execute()
assert len(return_nodes) == 1, 'traversal returned incorrect #nodes'
assert 'ayush' in return_nodes, 'traversal returned bad node handle'

# try to create node with same handle as before
c.begin_tx()
c.create_node('ayush')
assert not c.end_tx(), 'create node passed'

# try to create edge with same handle as before
c.begin_tx()
c.create_edge('ayush', 'egs', 'e1')
assert not c.end_tx(), 'create edge passed'

print 'Correctly executed 8 transactions of varying complexity, pass simple_test.'
print 'Success, you have a working Weaver setup!'
