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
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.


cdef extern from "stdint.h":

    ctypedef short int int16_t
    ctypedef unsigned short int uint16_t
    ctypedef int int32_t
    ctypedef unsigned int uint32_t
    ctypedef long int int64_t
    ctypedef unsigned long int uint64_t
    ctypedef long unsigned int size_t


cdef extern from "stdlib.h":

    void* malloc(size_t size)
    void free(void* ptr)


cdef extern from "chronos.h":

    cdef struct chronos_client

    cdef enum chronos_returncode:
        CHRONOS_SUCCESS
        CHRONOS_ERROR
        CHRONOS_GARBAGE

    cdef enum chronos_cmp:
        CHRONOS_HAPPENS_BEFORE
        CHRONOS_HAPPENS_AFTER
        CHRONOS_CONCURRENT
        CHRONOS_WOULDLOOP
        CHRONOS_NOEXIST

    cdef enum:
        CHRONOS_SOFT_FAIL = 1

    cdef struct chronos_pair:
        uint64_t lhs
        uint64_t rhs
        uint32_t flags
        chronos_cmp order

    cdef struct chronos_stats:
        uint64_t time
        uint64_t utime
        uint64_t stime
        uint32_t maxrss
        uint64_t events
        uint64_t count_create_event
        uint64_t count_acquire_references
        uint64_t count_release_references
        uint64_t count_query_order
        uint64_t count_assign_order

    chronos_client* chronos_client_create(char* host, uint16_t port, uint64_t num_vts)
    void chronos_client_destroy(chronos_client* client)

    int64_t chronos_create_event(chronos_client* client, chronos_returncode* status, uint64_t* event) nogil
    int64_t chronos_acquire_references(chronos_client* client, uint64_t* events, size_t events_sz, chronos_returncode* status, ssize_t* ret) nogil
    int64_t chronos_release_references(chronos_client* client, uint64_t* events, size_t events_sz, chronos_returncode* status, ssize_t* ret) nogil
    int64_t chronos_query_order(chronos_client* client, chronos_pair* pairs, size_t pairs_sz, chronos_returncode* status, ssize_t* ret) nogil
    int64_t chronos_assign_order(chronos_client* client, chronos_pair* pairs, size_t pairs_sz, chronos_returncode* status, ssize_t* ret) nogil
    int64_t chronos_get_stats(chronos_client* client, chronos_returncode* status, chronos_stats* st, ssize_t* ret) nogil
    int64_t chronos_loop(chronos_client* client, int timeout, chronos_returncode* status) nogil
    int64_t chronos_wait(chronos_client* client, int64_t id, int timeout, chronos_returncode* status) nogil


ctypedef int64_t (*chronos_mod_references)(chronos_client* client, uint64_t* events, size_t events_sz, chronos_returncode* status, ssize_t* ret)
ctypedef int64_t (*chronos_order)(chronos_client* client, chronos_pair* pairs, size_t pairs_sz, chronos_returncode* status, ssize_t* ret)


cdef __enumtosymb(chronos_cmp c):
    return {CHRONOS_HAPPENS_BEFORE: '<',
            CHRONOS_HAPPENS_AFTER: '>',
            CHRONOS_CONCURRENT: '?',
            CHRONOS_WOULDLOOP: 'O',
            CHRONOS_NOEXIST: 'X'}.get(c, 'E')


cdef class Deferred:

    cdef Client _client
    cdef int64_t _reqid
    cdef chronos_returncode _status
    cdef bint _finished

    def __cinit__(self, Client client, *args):
        self._client = client
        self._reqid = 0
        self._status = CHRONOS_GARBAGE
        self._finished = False

    def _callback(self):
        self._finished = True
        del self._client._ops[self._reqid]

    def wait(self):
        while not self._finished and self._reqid > 0:
            self._client.loop(self._reqid)
        self._finished = True


cdef class DeferredCreateEvent(Deferred):

    cdef uint64_t _event

    def __cinit__(self, Client client):
        self._event = 0
        self._reqid = chronos_create_event(client._client, &self._status, &self._event)
        if self._reqid < 0:
            raise RuntimeError("it failed")
        client._ops[self._reqid] = self

    def wait(self):
        Deferred.wait(self)
        return self._event


cdef class DeferredReferences(Deferred):

    cdef uint64_t* _events
    cdef size_t _events_sz
    cdef ssize_t _ret

    def __cinit__(self, Client client):
        self._events = NULL
        self._events_sz = 0
        self._ret = 0

    def __dealloc__(self):
        if self._events != NULL:
            free(self._events)

    cdef call(self, chronos_mod_references op, events):
        self._events_sz = len(events)
        self._events = <uint64_t*> malloc(self._events_sz * sizeof(uint64_t))
        if self._events == NULL:
            raise MemoryError()
        for i, e in enumerate(events):
            self._events[i] = e
        self._reqid = op(self._client._client,
                         self._events, self._events_sz,
                         &self._status, &self._ret)
        if self._reqid < 0:
            raise RuntimeError("it failed")
        self._client._ops[self._reqid] = self

    def wait(self):
        Deferred.wait(self)
        if self._ret < 0:
            raise RuntimeError("it failed")
        elif self._ret < self._events_sz:
            raise RuntimeError("it failed to acquire reference to %i".format(self._events[self._ret]))
        else:
            return True


cdef class DeferredOrder(Deferred):

    cdef chronos_pair* _pairs
    cdef size_t _pairs_sz
    cdef ssize_t _ret

    def __cinit__(self, Client client):
        self._pairs = NULL
        self._pairs_sz = 0
        self._ret = 0

    def __dealloc__(self):
        if self._pairs != NULL:
            free(self._pairs)

    cdef call(self, chronos_order op, events):
        self._pairs_sz = len(events)
        self._pairs = <chronos_pair*> malloc(self._pairs_sz * sizeof(chronos_pair))
        if self._pairs == NULL:
            raise MemoryError()
        for i, ev in enumerate(events):
            self.check_event(ev)
            lhs, rhs, flags, order = self.parse_event(ev)
            self._pairs[i].lhs = lhs
            self._pairs[i].rhs = rhs
            self._pairs[i].flags = flags
            self._pairs[i].order = order
        self._reqid = op(self._client._client,
                         self._pairs, self._pairs_sz,
                         &self._status, &self._ret)
        if self._reqid < 0:
            raise RuntimeError("it failed")
        self._client._ops[self._reqid] = self

    def wait(self):
        Deferred.wait(self)
        if self._ret < 0:
            raise RuntimeError("it failed")
        else:
            self.check_result()
            return [(self._pairs[i].lhs, self._pairs[i].rhs, __enumtosymb(self._pairs[i].order))
                    for i in range(self._pairs_sz)]


cdef class DeferredQueryOrder(DeferredOrder):

    def check_event(self, ev):
        if len(ev) != 2:
            raise ValueError("Events need to be 2-tuples")

    def parse_event(self, ev):
        lhs, rhs = ev
        return lhs, rhs, 0, CHRONOS_CONCURRENT

    def check_result(self):
        pass


cdef class DeferredAssignOrder(DeferredOrder):

    def check_event(self, ev):
        if len(ev) != 3:
            raise ValueError("Events need to be 3-tuples")

    def parse_event(self, ev):
        lhs, rhs, other = ev
        flags = 0
        if 'f' in other:
            flags |= CHRONOS_SOFT_FAIL
        order = {'<': CHRONOS_HAPPENS_BEFORE,
                 '>': CHRONOS_HAPPENS_AFTER,
                 '': CHRONOS_HAPPENS_BEFORE}.get(other[:1], CHRONOS_NOEXIST)
        if order == CHRONOS_NOEXIST:
            raise ValueError("Order should be '<' or '>'")
        return lhs, rhs, flags, order

    def check_result(self):
        for i in range(self._pairs_sz):
            if self._pairs[i].order == CHRONOS_WOULDLOOP:
                raise RuntimeError("it failed")


cdef class DeferredGetStats(Deferred):

    cdef chronos_stats _st
    cdef ssize_t _ret

    def __cinit__(self, Client client):
        self._ret = 0
        self._reqid = chronos_get_stats(client._client, &self._status, &self._st, &self._ret)
        if self._reqid < 0:
            raise RuntimeError("it failed")
        client._ops[self._reqid] = self

    def wait(self):
        Deferred.wait(self)
        if self._ret != 0:
            raise RuntimeError("it failed")
        return {'time': self._st.time,
                'utime': self._st.utime,
                'stime': self._st.stime,
                'maxrss': self._st.maxrss,
                'events': self._st.events,
                'count_create_event': self._st.count_create_event,
                'count_acquire_references': self._st.count_acquire_references,
                'count_release_references': self._st.count_release_references,
                'count_query_order': self._st.count_query_order,
                'count_assign_order': self._st.count_assign_order}


cdef class Client:

    cdef chronos_client* _client
    cdef dict _ops

    def __cinit__(self, host, port, num_vts):
        self._client = chronos_client_create(host, port, num_vts)
        self._ops = {}
        if self._client == NULL:
            raise OSError("Could not connect")

    def __dealloc__(self):
        if self._client != NULL:
            chronos_client_destroy(self._client)

    def create_event(self):
        return self.async_create_event().wait()

    def acquire_references(self, events):
        return self.async_acquire_references().wait()

    def release_references(self, events):
        return self.async_release_references(events).wait()

    def query_order(self, events):
        return self.async_query_order(events).wait()

    def assign_order(self, events):
        return self.async_assign_order(events).wait()

    def get_stats(self):
        return self.async_get_stats().wait()

    def async_create_event(self):
        return DeferredCreateEvent(self)

    def async_acquire_references(self, events):
        d = DeferredReferences(self)
        d.call(<chronos_mod_references> chronos_acquire_references, events)
        return d

    def async_release_references(self, events):
        d = DeferredReferences(self)
        d.call(<chronos_mod_references> chronos_release_references, events)
        return d

    def async_query_order(self, events):
        d = DeferredQueryOrder(self)
        d.call(<chronos_order> chronos_query_order, events)
        return d

    def async_assign_order(self, events):
        d = DeferredAssignOrder(self)
        d.call(<chronos_order> chronos_assign_order, events)
        return d

    def async_get_stats(self):
        return DeferredGetStats(self)

    def loop(self, reqid=None):
        cdef chronos_returncode rc
        if reqid is None:
            ret = chronos_loop(self._client, -1, &rc)
        else:
            ret = chronos_wait(self._client, reqid, -1, &rc)
        if ret < 0:
            raise RuntimeError("it failed")
        else:
            assert ret in self._ops
            op = self._ops[ret]
            # We cannot refer to self._ops[ret] after this call as
            # _callback() may remove ret from self._ops.
            op._callback()
            return op
