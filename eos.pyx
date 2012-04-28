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

cdef extern from "eos.h":

    cdef struct eos_client

    cdef enum eos_cmp:
        EOS_HAPPENS_BEFORE
        EOS_HAPPENS_AFTER
        EOS_CONCURRENT
        EOS_NOEXIST

    cdef enum:
        EOS_SOFT_FAIL = 1

    cdef struct eos_pair:
        uint64_t lhs
        uint64_t rhs
        uint32_t flags
        eos_cmp order

    cdef struct eos_stats:
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

    eos_client* eos_client_create(char* host, uint16_t port)
    void eos_client_destroy(eos_client* client)

    uint64_t eos_create_event(eos_client* client) nogil
    int eos_acquire_references(eos_client* client, uint64_t* events, size_t events_sz) nogil
    int eos_release_references(eos_client* client, uint64_t* events, size_t events_sz) nogil
    int eos_query_order(eos_client* client, eos_pair* pairs, size_t pairs_sz) nogil
    int eos_assign_order(eos_client* client, eos_pair* pairs, size_t pairs_sz) nogil
    int eos_get_stats(eos_client* client, eos_stats* st) nogil

cdef __enumtosymb(eos_cmp c):
    return {EOS_HAPPENS_BEFORE: '<',
            EOS_HAPPENS_AFTER: '>',
            EOS_CONCURRENT: '?',
            EOS_NOEXIST: 'X'}.get(c, 'E')

cdef class Client:
    cdef eos_client* _client

    def __cinit__(self, host, port):
        self._client = eos_client_create(host, port)
        if self._client == NULL:
            raise OSError("Could not connect")

    def __dealloc__(self):
        if self._client != NULL:
            eos_client_destroy(self._client)

    def create_event(self):
        cdef uint64_t event
        event = eos_create_event(self._client);
        if event > 0:
            return event
        return None # XXX Exception

    def acquire_references(self, events):
        cdef uint64_t* _events
        _events = <uint64_t*> malloc(sizeof(uint64_t) * len(events))
        if _events == NULL:
            raise MemoryError()
        try:
            for i, e in enumerate(events):
                _events[i] = e
            ret = eos_release_references(self._client, _events, len(events))
            if ret < 0:
                raise RuntimeError("it failed")
            if ret > 0:
                raise RuntimeError("it failed to acquire reference to %i".format(_events[ret - 1]))
        finally:
            free(_events)

    def release_references(self, events):
        cdef uint64_t* _events
        _events = <uint64_t*> malloc(sizeof(uint64_t) * len(events))
        if _events == NULL:
            raise MemoryError()
        try:
            for i, e in enumerate(events):
                _events[i] = e
            ret = eos_release_references(self._client, _events, len(events))
            if ret != 0:
                raise RuntimeError("it failed")
        finally:
            free(_events)

    def query_order(self, events):
        cdef eos_pair* pairs
        cdef tuple ev
        pairs = <eos_pair*> malloc(sizeof(eos_pair) * len(events))
        if pairs == NULL:
            raise MemoryError()
        try:
            for i, ev in enumerate(events):
                if len(ev) != 2:
                    raise ValueError("Events need to be 2-tuples")
                lhs, rhs = ev
                pairs[i].lhs = lhs
                pairs[i].rhs = rhs
                pairs[i].flags = 0
                pairs[i].order = EOS_CONCURRENT
            ret = eos_query_order(self._client, pairs, len(events))
            if ret != 0:
                raise RuntimeError("it failed")
            return [(pairs[i].lhs, pairs[i].rhs, __enumtosymb(pairs[i].order)) for i in range(len(events))]
        finally:
            free(pairs)

    def assign_order(self, events):
        cdef eos_pair* pairs
        cdef tuple ev
        cdef uint64_t lhs
        cdef uint64_t rhs
        cdef bytes other
        pairs = <eos_pair*> malloc(sizeof(eos_pair) * len(events))
        if pairs == NULL:
            raise MemoryError()
        try:
            for i, ev in enumerate(events):
                if len(ev) != 3:
                    raise ValueError("Events need to be 3-tuples")
                lhs, rhs, other = ev
                pairs[i].lhs = lhs
                pairs[i].rhs = rhs
                pairs[i].flags = 0
                if 'f' in other:
                    pairs[i].flags |= EOS_SOFT_FAIL
                pairs[i].order = {'<': EOS_HAPPENS_BEFORE,
                                  '>': EOS_HAPPENS_AFTER,
                                  '': EOS_HAPPENS_BEFORE}.get(other[:1], EOS_NOEXIST)
                if pairs[i].order == EOS_NOEXIST:
                    raise ValueError("Order should be '<' or '>'")
            ret = eos_assign_order(self._client, pairs, len(events))
            if ret != 0:
                raise RuntimeError("it failed")
            return [(pairs[i].lhs, pairs[i].rhs, __enumtosymb(pairs[i].order)) for i in range(len(events))]
        finally:
            free(pairs)

    def get_stats(self):
        cdef eos_stats st
        ret = eos_get_stats(self._client, &st)
        if ret != 0:
            raise RuntimeError("it failed")
        return {'time': st.time,
                'utime': st.utime,
                'stime': st.stime,
                'maxrss': st.maxrss,
                'events': st.events,
                'count_create_event': st.count_create_event,
                'count_acquire_references': st.count_acquire_references,
                'count_release_references': st.count_release_references,
                'count_query_order': st.count_query_order,
                'count_assign_order': st.count_assign_order}
