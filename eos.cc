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
//     * Neither the name of HyperDex nor the names of its contributors may be
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

// eos
#include "eos.h"
#include "eos_cmp_encode.h"
#include "network_constants.h"

class eos_client
{
    public:
        eos_client(po6::net::location where);
        ~eos_client() throw ();

    public:
        po6::net::socket sock;
        uint64_t nonce;
};

eos_client :: eos_client(po6::net::location where)
    : sock(where.address.family(), SOCK_STREAM, IPPROTO_TCP)
    , nonce(1)
{
    sock.connect(where);
}

eos_client :: ~eos_client() throw ()
{
}

extern "C"
{

eos_client*
eos_client_create(const char* host, uint16_t port)
{
    try
    {
        po6::net::location loc(host, port);
        std::auto_ptr<eos_client> c(new eos_client(loc));
        return c.release();
    }
    catch (po6::error& e)
    {
        errno = e;
        return NULL;
    }
}

void
eos_client_destroy(eos_client* client)
{
    if (client)
    {
        delete client;
    }
}

uint64_t
eos_create_event(struct eos_client* client)
{
    const ssize_t SEND_MSG_SIZE = sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint64_t);
    const ssize_t RECV_MSG_SIZE = sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t);
    uint8_t buffer[RECV_MSG_SIZE]; // need to use the larger of the two
    uint8_t* ptmp = buffer;
    ptmp = e::pack32be(SEND_MSG_SIZE, ptmp);
    ptmp = e::pack16be(static_cast<uint16_t>(EOSNC_CREATE_EVENT), ptmp);
    ptmp = e::pack64be(client->nonce, ptmp);
    ++client->nonce;
    ssize_t ret;
    ret = client->sock.send(buffer, SEND_MSG_SIZE, MSG_WAITALL);

    if (ret != SEND_MSG_SIZE)
    {
        return 0;
    }

    ret = client->sock.recv(buffer, RECV_MSG_SIZE, MSG_WAITALL);

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

    if (nonce + 1 != client->nonce)
    {
        return 0;
    }

    return event;
}

int
eos_query_order(struct eos_client* client, struct eos_pair* pairs, size_t pairs_sz)
{
    const ssize_t SEND_MSG_SIZE = sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint64_t)
                                + (sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t)) * pairs_sz;
    const ssize_t RECV_MSG_SIZE = sizeof(uint32_t) + sizeof(uint64_t)
                                + sizeof(uint8_t) * pairs_sz;
    std::vector<uint8_t> buffer(SEND_MSG_SIZE);
    uint8_t* ptmp = &buffer.front();
    ptmp = e::pack32be(SEND_MSG_SIZE, ptmp);
    ptmp = e::pack16be(static_cast<uint16_t>(EOSNC_QUERY_ORDER), ptmp);
    ptmp = e::pack64be(client->nonce, ptmp);
    ++client->nonce;

    for (size_t i = 0; i < pairs_sz; ++i)
    {
        ptmp = e::pack64be(pairs[i].lhs, ptmp);
        ptmp = e::pack64be(pairs[i].rhs, ptmp);
        ptmp = e::pack32be(pairs[i].flags, ptmp);
    }

    ssize_t ret;
    ret = client->sock.send(&buffer.front(), SEND_MSG_SIZE, MSG_WAITALL);

    if (ret != SEND_MSG_SIZE)
    {
        return -1;
    }

    ret = client->sock.recv(&buffer.front(), RECV_MSG_SIZE, MSG_WAITALL);

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

    if (nonce + 1 != client->nonce)
    {
        return -1;
    }

    for (size_t i = 0; i < pairs_sz; ++i)
    {
        pairs[i].order = byte_to_eos_cmp(utmp[i]);
    }

    return 0;
}

int
eos_assign_order(struct eos_client* client, struct eos_pair* pairs, size_t pairs_sz)
{
    const ssize_t SEND_MSG_SIZE = sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint64_t)
                                + (sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint8_t)) * pairs_sz;
    const ssize_t RECV_MSG_SIZE = sizeof(uint32_t) + sizeof(uint64_t)
                                + sizeof(uint8_t) * pairs_sz;
    std::vector<uint8_t> buffer(SEND_MSG_SIZE);
    uint8_t* ptmp = &buffer.front();
    ptmp = e::pack32be(SEND_MSG_SIZE, ptmp);
    ptmp = e::pack16be(static_cast<uint16_t>(EOSNC_ASSIGN_ORDER), ptmp);
    ptmp = e::pack64be(client->nonce, ptmp);
    ++client->nonce;

    for (size_t i = 0; i < pairs_sz; ++i)
    {
        ptmp = e::pack64be(pairs[i].lhs, ptmp);
        ptmp = e::pack64be(pairs[i].rhs, ptmp);
        ptmp = e::pack32be(pairs[i].flags, ptmp);
        *ptmp = eos_cmp_to_byte(pairs[i].order);
        ++ptmp;
    }

    ssize_t ret;
    ret = client->sock.send(&buffer.front(), SEND_MSG_SIZE, MSG_WAITALL);

    if (ret != SEND_MSG_SIZE)
    {
        return -1;
    }

    ret = client->sock.recv(&buffer.front(), RECV_MSG_SIZE, MSG_WAITALL);

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

    if (nonce + 1 != client->nonce)
    {
        return -1;
    }

    for (size_t i = 0; i < pairs_sz; ++i)
    {
        pairs[i].order = byte_to_eos_cmp(utmp[i]);

        if (pairs[i].order == EOS_CONCURRENT || pairs[i].order == EOS_NOEXIST)
        {
            return i + 1;
        }
    }

    return 0;
}

}
