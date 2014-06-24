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

import sys
import libweaverpyclient as client

# create client object
c = client.Client(client.CL_ID, 0)

id_to_name = {}

# create node for user ayush
c.begin_tx()
ayush = c.create_node()
c.set_node_property(ayush, 'type', 'user')
c.end_tx()
print 'User ayush has user id ' + str(ayush)
id_to_name[ayush] = 'ayush'

# create node for user egs
c.begin_tx()
egs = c.create_node()
c.set_node_property(egs, 'type', 'user')
c.end_tx()
print 'User egs has user id ' + str(egs)
id_to_name[egs] = 'egs'

# ayush follows egs
c.begin_tx()
forward_edge = c.create_edge(ayush, egs)
c.set_edge_property(ayush, forward_edge, 'type', 'follows')
reverse_edge = c.create_edge(egs, ayush)
c.set_edge_property(egs, reverse_edge, 'type', 'followed_by')
c.end_tx()
print 'ayush follows egs'

# add a post and restrict visibility to followers only
c.begin_tx()
post = c.create_node()
c.set_node_property(post, 'type', 'post')
c.set_node_property(post, 'visibility', 'followers')
post_edge = c.create_edge(egs, post)
c.set_edge_property(egs, post_edge, 'type', 'posted')
c.end_tx()
print 'egs posted content'

# 'like' the post
c.begin_tx()
like_edge = c.create_edge(post, ayush)
c.set_edge_property(post, like_edge, 'type', 'liked_by')
c.end_tx()
print 'ayush likes egs\'s post'

# list all the people who like egs's post
# for every hop, list the desired node properties
NodeProps = [[('type', 'user')], [('type', 'post')], [('type', 'user')]]
# for every hop, list the desired out-edge properties
EdgeProps = [[('type', 'posted')], [('type', 'liked_by')]]
params = client.TraversePropsParams(node_props=NodeProps, edge_props=EdgeProps)
response = c.traverse_props([(egs, params)])
print 'List of users who like egs\'s posts:'
for user in response.return_nodes:
    print '\t' + id_to_name[user]
