#! /usr/bin/env python
# 
# ===============================================================
# Copyright (C) 2013, Cornell University, see the LICENSE file
#                     for licensing agreement
# ===============================================================
# 

import sys
import time
sys.path.append('../../bindings/python')

import client

class simple_client:
    def __init__(self, client_to_wrap):
        self.c= client_to_wrap

    def node_props(self):
        print 'temp'

    def edges_props(self):
        print 'temp'

    def reachability(self, source, dest, edge_props = []):
        rp = client.ReachParams(dest=dest, edge_props=edge_props)
        prog_args = [(source, rp)]
        response = self.c.run_reach_program(prog_args)
        print "got response"
        return response.reachable

    def clustering(self):
        print 'temp'
