# Copyright (c) 2012, Cornell University
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 
#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of HyperDex nor the names of its contributors may be
#       used to endorse or promote products derived from this software without
#       specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
# ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


'''Evaluate the effects of random edge'''


import itertools
import random
import sys

import chronos
import chronosbench
import chronosbench.util


class RandomGraph(object):

    def __init__(self, daemon, edgefilename, num_events, group_sz):
        self._client = chronos.Client(daemon.host(), daemon.port())
        self._num_events = num_events
        self._group_sz = group_sz
        self._edgefile = open(edgefilename, 'r')

    def create_events(self):
        for i in range(self._num_events):
            ev = self._client.create_event()
            assert 0 < ev <= self._num_events

    def create_edges(self):
        def direction_gen():
            exp = [2**i for i in range(128)]
            while True:
                bits = random.getrandbits(128)
                for i in range(128):
                    if exp[i] & bits:
                        yield '<f'
                    else:
                        yield '>f'
        def edges():
            direction = direction_gen()
            for line in self._edgefile:
                u, v = line[:-1].split(' ')
                u, v = int(u), int(v)
                u, v = u + 1, v + 1
                yield u, v, direction.next()
        for assignments in chronosbench.util.group_into_lists(edges(), self._group_sz, None):
            while assignments and assignments[-1] is None:
                assignments = assignments[:-1]
            if assignments:
                self._client.assign_order(assignments)

    def run(self, fout=None):
        fout = fout or sys.stderr
        self.create_events()
        self.create_edges()

        # Find the edges we want to query
        idxs = set(random.sample(xrange(49995000), 100000))
        edges = []
        for idx, edge in enumerate(itertools.combinations(range(10000), 2)):
            if idx in idxs:
                u, v = edge
                u, v = u + 1, v + 1
                edges.append(edge)

        # Run the queries
        print >>fout, self._client.get_stats()
        concurrent = 0
        for queries in chronosbench.util.group_into_lists(edges, self._group_sz, None):
            while queries and queries[-1] is None:
                queries = queries[:-1]
            if queries:
                orders = self._client.query_order(queries)
                concurrent += len([x for (x, y, z) in orders if z == '?'])
        stats = self._client.get_stats()
        stats['concurrent'] = concurrent
        print >>fout, stats


if __name__ == '__main__':
    import sys
    graphname = sys.argv[1]
    with chronosbench.ChronosDaemon() as daemon:
        rg = RandomGraph(daemon, graphname, 100000, 600)
        with chronosbench.DataOut(graphname + '.result') as fout:
            if not fout:
                sys.exit(0)
            rg.run(fout)
