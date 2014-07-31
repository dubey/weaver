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


'''Evaluate the system scalability'''


import itertools
import random
import sys
import time

import chronos
import chronosbench
import chronosbench.util


class ScalabilityLoad(object):

    def __init__(self, host, port, num_events, probability):
        self._client = chronos.Client(host, port)
        self._num_events = num_events
        self._probability = probability

    def run(self):
        for i in range(self._num_events):
            ev = self._client.create_event()
            assert ev == i + 1
        def direction_gen():
            exp = [2**i for i in range(128)]
            while True:
                bits = random.getrandbits(128)
                for i in range(128):
                    if exp[i] & bits:
                        yield '<f'
                    else:
                        yield '>f'
        direction = direction_gen()
        for edge in itertools.combinations(range(10000), 2):
            if random.random() < self._probability:
                u, v = edge
                u, v = u + 1, v + 1
                self._client.assign_order([(u, v, direction.next())])


class ScalabilityClient(object):

    def __init__(self, host, port, num_events, run_for):
        self._client = chronos.Client(host, port)
        self._num_events = num_events
        self._run_for = run_for

    def run(self):
        start = time.time()
        target = float(int(start)) + 0.5
        now = start
        ops = 0
        fail = 0
        def edges():
            while True:
                u = random.randint(1, self._num_events)
                v = random.randint(1, self._num_events)
                yield u, v
        queries = iter(chronosbench.util.group_into_lists(edges(), 100, None))
        while start + self._run_for > now:
            now = time.time()
            if now > target:
                print int(target), ops, fail
                target += 1
                ops = 0
                fail = 0
            query = queries.next()
            self._client.query_order(query)
            ops += 100


if __name__ == '__main__':
    host = sys.argv[2]
    port = int(sys.argv[3])
    num_events = int(sys.argv[4])
    if sys.argv[1] == 'client':
        run_for = int(sys.argv[5])
        sc = ScalabilityClient(host, port, num_events, run_for)
        sc.run()
    elif sys.argv[1] == 'load':
        probability = float(sys.argv[5])
        sl = ScalabilityLoad(host, port, num_events, probability)
        sl.run()
        print 'Done loading'
        while True:
            time.sleep(1)
