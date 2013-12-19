#! /usr/bin/env python
# 
# ===============================================================
#    Description:  Tranform SNAP graph files so that node handles
#                  are range(0, num_nodes).
# 
#        Created:  12/03/2013 02:17:15 PM
# 
#         Author:  Ayush Dubey, dubey@cs.cornell.edu
# 
# Copyright (C) 2013, Cornell University, see the LICENSE file
#                     for licensing agreement
# ===============================================================
# 

import sys
from sets import Set

def snap_to_weaver(snap_file):
    path = snap_file.split('/')
    filename = path[len(path)-1]
    path[len(path)-1] = 'weaver'
    path.append(filename)
    weaver_file = '/'.join(path)

    sfile = open(snap_file, 'r')
    nmap = {}
    edges = {}
    handle = 0
    for line in sfile:
        if len(line) == 0 or line[0] == '#':
            continue
        split = line.split()
        if len(split) != 2:
            print 'Invalid snap file'
            return
        n1 = int(split[0])
        n2 = int(split[1])
        if not n1 in nmap:
            nmap[n1] = handle
            edges[handle] = Set([])
            handle += 1
        if not n2 in nmap:
            nmap[n2] = handle
            edges[handle] = Set([])
            handle += 1
        edges[nmap[n1]].add(nmap[n2])
    sfile.close()

    wfile = open(weaver_file, 'w')
    wfile.write('#' + str(len(nmap)) + '\n')
    for n1 in edges:
        for n2 in edges[n1]:
            wfile.write(str(n1) + ' ' + str(n2) + '\n')
    wfile.close()

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print 'Needs to be called with only a single argument -- the absolute location of the snap graph file'
    else:
        snap_to_weaver(sys.argv[1])
