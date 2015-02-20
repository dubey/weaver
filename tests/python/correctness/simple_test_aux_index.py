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

# 1. create node for user ayush
c.begin_tx()
c.create_node('ayush')
c.set_node_property('type', 'user', 'ayush')
assert c.end_tx(), 'create node failed'

# 2. create node for user egs
c.begin_tx()
c.create_node('egs')
c.set_node_property('type', 'user', 'egs')
assert c.end_tx(), 'create node failed'

# 3. ayush follows egs
c.begin_tx()
c.create_edge('ayush', 'egs', 'e1')
c.set_edge_property(edge='e1', key='type', value='follows')
c.create_edge('egs', 'ayush', 'e2')
c.set_edge_property(edge='e2', key='type', value='followed_by')
assert c.end_tx(), 'tx fail, something is wrong'

# 4. add a post and restrict visibility to followers only
c.begin_tx()
c.create_node('post')
c.set_node_property('type', 'post', 'post')
c.set_node_property('visibility', 'followers', 'post')
e3 = c.create_edge('egs', 'post')
c.set_edge_property(edge=e3, key='type', value='posted')
assert c.end_tx(), 'tx fail, something is wrong'

# 5. 'like' the post
c.begin_tx()
e4 = c.create_edge('post', 'ayush')
c.set_edge_property(edge=e4, key='type', value='liked_by')
assert c.end_tx(), 'create edge failed'

# 6. list all the people who like egs's post
return_nodes = c.traverse('egs', {'type': 'user'}).out_edge({'type': 'posted'}).node({'type': 'post'}).out_edge({'type': 'liked_by'}).node({'type': 'user'}).execute()
assert len(return_nodes) == 1, 'traversal returned incorrect #nodes'
assert 'ayush' in return_nodes, 'traversal returned bad node handle'

# 7. try to create node with same handle as before
c.begin_tx()
c.create_node('ayush')
assert not c.end_tx(), 'create node passed'

# 8. try to create edge with same handle as before
c.begin_tx()
c.create_edge('ayush', 'egs', 'e1')
assert not c.end_tx(), 'create edge passed'

# 9. add auxiliary handles to nodes
c.begin_tx()
c.add_alias('ad688', 'ayush')
c.add_alias('el33th4x0r', 'egs')
assert c.end_tx(), 'add aux handles'

# 10. list all the people who like egs's post
# this time with aliases instead of handles
return_nodes = c.traverse('el33th4x0r', {'type': 'user'}).out_edge({'type': 'posted'}).node({'type': 'post'}).out_edge({'type': 'liked_by'}).node({'type': 'user'}).execute()
assert len(return_nodes) == 1, 'traversal returned incorrect #nodes'
assert 'ayush' in return_nodes, 'traversal returned bad node handle'

# 11. get node and check it is valid
ad = c.get_node('ayush')
assert 'ad688' in ad.aliases

print 'Correctly executed 11 transactions of varying complexity, pass simple_test.'
print 'Success, you have a working Weaver setup!'
