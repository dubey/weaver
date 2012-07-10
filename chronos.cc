// Copyright (c) 2012, Cornell University
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of Chronos nor the names of its contributors may be
//       used to endorse or promote products derived from this software without
//       specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

// STL
#include <memory>
#include <vector>

#include <iostream>

// po6
#include <po6/net/location.h>
#include <po6/net/socket.h>

// e
#include <e/endian.h>

// Chronos
#include "chronos.h"
#include "chronos_cmp_encode.h"
#include "network_constants.h"

chronos_client :: chronos_client(const char* host, uint16_t port)
    : m_where(host, port)
    , m_sock(m_where.address.family(), SOCK_STREAM, IPPROTO_TCP)
    , m_nonce(1)
{
    m_sock.connect(m_where);
}

chronos_client :: ~chronos_client() throw ()
{
}

uint64_t
chronos_client :: create_event()
{
    const ssize_t SEND_MSG_SIZE = sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint64_t);
    const ssize_t RECV_MSG_SIZE = sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t);
    uint8_t buffer[RECV_MSG_SIZE]; // need to use the larger of the two
    uint8_t* ptmp = buffer;
    ptmp = e::pack32be(SEND_MSG_SIZE, ptmp);
    ptmp = e::pack16be(static_cast<uint16_t>(CHRONOSNC_CREATE_EVENT), ptmp);
    ptmp = e::pack64be(m_nonce, ptmp);
    ++m_nonce;
    ssize_t ret;
    ret = m_sock.send(buffer, SEND_MSG_SIZE, MSG_WAITALL);

    if (ret != SEND_MSG_SIZE)
    {
        return 0;
    }

    ret = m_sock.recv(buffer, RECV_MSG_SIZE, MSG_WAITALL);

    if (ret != RECV_MSG_SIZE)
    {
        return 0;
    }

    uint32_t size = 0;
    uint64_t nonce = 0;
    uint64_t event = 0;
    const uint8_t* utmp = buffer;
    utmp = e::unpack32be(utmp, &size);
    utmp = e::unpack64be(utmp, &nonce);
    utmp = e::unpack64be(utmp, &event);

    if (size != RECV_MSG_SIZE)
    {
        return 0;
    }

    if (nonce + 1 != m_nonce)
    {
        return 0;
    }

    return event;
}

int
chronos_client :: acquire_references(uint64_t* events, size_t events_sz)
{
    const ssize_t SEND_MSG_SIZE = sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint64_t)
                                + sizeof(uint64_t) * events_sz;
    const ssize_t RECV_MSG_SIZE = sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t);
    std::vector<uint8_t> buffer(SEND_MSG_SIZE);
    uint8_t* ptmp = &buffer.front();
    ptmp = e::pack32be(SEND_MSG_SIZE, ptmp);
    ptmp = e::pack16be(static_cast<uint16_t>(CHRONOSNC_ACQUIRE_REF), ptmp);
    ptmp = e::pack64be(m_nonce, ptmp);
    ++m_nonce;

    for (size_t i = 0; i < events_sz; ++i)
    {
        ptmp = e::pack64be(events[i], ptmp);
    }

    ssize_t ret;
    ret = m_sock.send(&buffer.front(), SEND_MSG_SIZE, MSG_WAITALL);

    if (ret != SEND_MSG_SIZE)
    {
        return -1;
    }

    ret = m_sock.recv(&buffer.front(), RECV_MSG_SIZE, MSG_WAITALL);

    if (ret != RECV_MSG_SIZE)
    {
        return -1;
    }

    uint32_t size = 0;
    uint64_t nonce = 0;
    uint64_t num = 0;
    const uint8_t* utmp = &buffer.front();
    utmp = e::unpack32be(utmp, &size);
    utmp = e::unpack64be(utmp, &nonce);
    utmp = e::unpack64be(utmp, &num);

    if (size != RECV_MSG_SIZE)
    {
        return -1;
    }

    if (nonce + 1 != m_nonce)
    {
        return -1;
    }

    if (num != events_sz)
    {
        return num + 1;
    }

    return 0;
}

int
chronos_client :: release_references(uint64_t* events, size_t events_sz)
{
    const ssize_t SEND_MSG_SIZE = sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint64_t)
                                + sizeof(uint64_t) * events_sz;
    const ssize_t RECV_MSG_SIZE = sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t);
    std::vector<uint8_t> buffer(SEND_MSG_SIZE);
    uint8_t* ptmp = &buffer.front();
    ptmp = e::pack32be(SEND_MSG_SIZE, ptmp);
    ptmp = e::pack16be(static_cast<uint16_t>(CHRONOSNC_RELEASE_REF), ptmp);
    ptmp = e::pack64be(m_nonce, ptmp);
    ++m_nonce;

    for (size_t i = 0; i < events_sz; ++i)
    {
        ptmp = e::pack64be(events[i], ptmp);
    }

    ssize_t ret;
    ret = m_sock.send(&buffer.front(), SEND_MSG_SIZE, MSG_WAITALL);

    if (ret != SEND_MSG_SIZE)
    {
        return -1;
    }

    ret = m_sock.recv(&buffer.front(), RECV_MSG_SIZE, MSG_WAITALL);

    if (ret != RECV_MSG_SIZE)
    {
        return -1;
    }

    uint32_t size = 0;
    uint64_t nonce = 0;
    uint64_t num = 0;
    const uint8_t* utmp = &buffer.front();
    utmp = e::unpack32be(utmp, &size);
    utmp = e::unpack64be(utmp, &nonce);
    utmp = e::unpack64be(utmp, &num);

    if (size != RECV_MSG_SIZE)
    {
        return -1;
    }

    if (nonce + 1 != m_nonce)
    {
        return -1;
    }

    return 0;
}

int
chronos_client :: query_order(chronos_pair* pairs, size_t pairs_sz)
{
    const ssize_t SEND_MSG_SIZE = sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint64_t)
                                + (sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t)) * pairs_sz;
    const ssize_t RECV_MSG_SIZE = sizeof(uint32_t) + sizeof(uint64_t)
                                + sizeof(uint8_t) * pairs_sz;
    std::vector<uint8_t> buffer(SEND_MSG_SIZE);
    uint8_t* ptmp = &buffer.front();
    ptmp = e::pack32be(SEND_MSG_SIZE, ptmp);
    ptmp = e::pack16be(static_cast<uint16_t>(CHRONOSNC_QUERY_ORDER), ptmp);
    ptmp = e::pack64be(m_nonce, ptmp);
    ++m_nonce;

    for (size_t i = 0; i < pairs_sz; ++i)
    {
        ptmp = e::pack64be(pairs[i].lhs, ptmp);
        ptmp = e::pack64be(pairs[i].rhs, ptmp);
        ptmp = e::pack32be(pairs[i].flags, ptmp);
    }

    ssize_t ret;
    ret = m_sock.send(&buffer.front(), SEND_MSG_SIZE, MSG_WAITALL);

    if (ret != SEND_MSG_SIZE)
    {
        return -1;
    }

    ret = m_sock.recv(&buffer.front(), RECV_MSG_SIZE, MSG_WAITALL);

    if (ret != RECV_MSG_SIZE)
    {
        return -1;
    }

    uint32_t size = 0;
    uint64_t nonce = 0;
    const uint8_t* utmp = &buffer.front();
    utmp = e::unpack32be(utmp, &size);
    utmp = e::unpack64be(utmp, &nonce);

    if (size != RECV_MSG_SIZE)
    {
        return -1;
    }

    if (nonce + 1 != m_nonce)
    {
        return -1;
    }

    for (size_t i = 0; i < pairs_sz; ++i)
    {
        pairs[i].order = byte_to_chronos_cmp(utmp[i]);
    }

    return 0;
}

int
chronos_client :: assign_order(chronos_pair* pairs, size_t pairs_sz)
{
    const ssize_t SEND_MSG_SIZE = sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint64_t)
                                + (sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint8_t)) * pairs_sz;
    const ssize_t RECV_MSG_SIZE = sizeof(uint32_t) + sizeof(uint64_t)
                                + sizeof(uint8_t) * pairs_sz;
    std::vector<uint8_t> buffer(SEND_MSG_SIZE);
    uint8_t* ptmp = &buffer.front();
    ptmp = e::pack32be(SEND_MSG_SIZE, ptmp);
    ptmp = e::pack16be(static_cast<uint16_t>(CHRONOSNC_ASSIGN_ORDER), ptmp);
    ptmp = e::pack64be(m_nonce, ptmp);
    ++m_nonce;

    for (size_t i = 0; i < pairs_sz; ++i)
    {
        ptmp = e::pack64be(pairs[i].lhs, ptmp);
        ptmp = e::pack64be(pairs[i].rhs, ptmp);
        ptmp = e::pack32be(pairs[i].flags, ptmp);
        *ptmp = chronos_cmp_to_byte(pairs[i].order);
        ++ptmp;
    }

    ssize_t ret;
    ret = m_sock.send(&buffer.front(), SEND_MSG_SIZE, MSG_WAITALL);

    if (ret != SEND_MSG_SIZE)
    {
        return -1;
    }

    ret = m_sock.recv(&buffer.front(), RECV_MSG_SIZE, MSG_WAITALL);

    if (ret != RECV_MSG_SIZE)
    {
        return -1;
    }

    uint32_t size = 0;
    uint64_t nonce = 0;
    const uint8_t* utmp = &buffer.front();
    utmp = e::unpack32be(utmp, &size);
    utmp = e::unpack64be(utmp, &nonce);

    if (size != RECV_MSG_SIZE)
    {
        return -1;
    }

    if (nonce + 1 != m_nonce)
    {
        return -1;
    }

    for (size_t i = 0; i < pairs_sz; ++i)
    {
        pairs[i].order = byte_to_chronos_cmp(utmp[i]);

        if (pairs[i].order == CHRONOS_CONCURRENT || pairs[i].order == CHRONOS_NOEXIST)
        {
            return i + 1;
        }
    }

    return 0;
}

int
chronos_client :: get_stats(chronos_stats* st)
{
    const ssize_t SEND_MSG_SIZE = sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint64_t);
    const ssize_t RECV_MSG_SIZE = sizeof(uint32_t) + sizeof(uint64_t)
                                + sizeof(uint32_t) + sizeof(uint64_t) * 9;
    uint8_t buffer[RECV_MSG_SIZE]; // need to use the larger of the two
    uint8_t* ptmp = buffer;
    ptmp = e::pack32be(SEND_MSG_SIZE, ptmp);
    ptmp = e::pack16be(static_cast<uint16_t>(CHRONOSNC_GET_STATS), ptmp);
    ptmp = e::pack64be(m_nonce, ptmp);
    ++m_nonce;
    ssize_t ret;
    ret = m_sock.send(buffer, SEND_MSG_SIZE, MSG_WAITALL);

    if (ret != SEND_MSG_SIZE)
    {
        return -1;
    }

    ret = m_sock.recv(buffer, RECV_MSG_SIZE, MSG_WAITALL);

    if (ret != RECV_MSG_SIZE)
    {
        return -1;
    }

    uint32_t size = 0;
    uint64_t nonce = 0;
    const uint8_t* utmp = buffer;
    utmp = e::unpack32be(utmp, &size);
    utmp = e::unpack64be(utmp, &nonce);
    utmp = e::unpack64be(utmp, &st->time);
    utmp = e::unpack64be(utmp, &st->utime);
    utmp = e::unpack64be(utmp, &st->stime);
    utmp = e::unpack32be(utmp, &st->maxrss);
    utmp = e::unpack64be(utmp, &st->events);
    utmp = e::unpack64be(utmp, &st->count_create_event);
    utmp = e::unpack64be(utmp, &st->count_acquire_references);
    utmp = e::unpack64be(utmp, &st->count_release_references);
    utmp = e::unpack64be(utmp, &st->count_query_order);
    utmp = e::unpack64be(utmp, &st->count_assign_order);

    if (size != RECV_MSG_SIZE)
    {
        return -1;
    }

    if (nonce + 1 != m_nonce)
    {
        return -1;
    }

    return 0;
}
