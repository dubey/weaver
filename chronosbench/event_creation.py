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


'''Evaluate the effects of event creation'''


import sys

import chronos
import chronosbench


class EventCreation(object):

    def __init__(self, daemon, num_events, every):
        self._client = chronos.Client(daemon.host(), daemon.port())
        self._num_events = num_events
        self._every = every

    def run(self, fout=None):
        fout = fout or sys.stderr
        print >>fout, self._client.get_stats()
        ev = self._client.create_event()
        ev_start = ev
        while ev < ev_start + self._num_events - 1:
            ev = self._client.create_event()
            if (ev - ev_start + 1) % self._every == 0:
                print >>fout, self._client.get_stats()


if __name__ == '__main__':
    for i in range(16):
        with chronosbench.ChronosDaemon() as daemon:
            ec = EventCreation(daemon, 10000, 100)
            with chronosbench.DataOut('create-1M-events-cold-%d.result' % i) as fout:
                if not fout:
                    continue
                ec.run(fout)
            with chronosbench.DataOut('create-1M-events-hot-%d.result' % i) as fout:
                if not fout:
                    continue
                ec.run(fout)
