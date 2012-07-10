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


'''Evaluate the effects of garbage collection'''


import itertools
import random
import sys

import chronos
import chronosbench


class GarbageCollection(object):

    def __init__(self, daemon):
        self._client = chronos.Client(daemon.host(), daemon.port())

    def create_chain(self, chain_length, release=True):
        '''Create a chain A->B->C where the only reference is to A.'''
        start = self._client.create_event()
        chain = []
        for i in range(chain_length):
            ev = self._client.create_event()
            chain.append(ev)
        self._client.assign_order([(start, chain[0], '<')])
        self._client.assign_order([(x, y, '<') for x, y in zip(chain, chain[1:])])
        if release:
            self._client.release_references(chain)
        return start

    def run(self, fout=None):
        fout = fout or sys.stderr
        for chain_length in [2**i for i in range(20)]:
            chains = []
            for i in range(100):
                chain = self.create_chain(chain_length, release=False)
                chains.append(chain)
            start = self._client.get_stats()
            for chain in chains:
                self._client.release_references([chain])
            end = self._client.get_stats()
            print >>fout, chain_length, (end['time'] - start['time']) / 100.
            fout.flush()


if __name__ == '__main__':
    import sys
    outfile = sys.argv[1]
    with chronosbench.ChronosDaemon() as daemon:
        rg = GarbageCollection(daemon)
        with chronosbench.DataOut(outfile) as fout:
            if not fout:
                sys.exit(0)
            rg.run(fout)
