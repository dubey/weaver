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

// Chronos
#include "chronos.h"

extern "C"
{

chronos_client*
chronos_client_create(const char* host, uint16_t port)
{
    try
    {
        return new chronos_client(host, port);
    }
    catch (po6::error& e)
    {
        errno = e;
        return NULL;
    }
    catch (...)
    {
        return NULL;
    }
}

void
chronos_client_destroy(chronos_client* client)
{
    try
    {
        if (client)
        {
            delete client;
        }
    }
    catch (...)
    {
    }
}

uint64_t
chronos_create_event(struct chronos_client* client)
{
    try
    {
        return client->create_event();
    }
    catch (po6::error& e)
    {
        errno = e;
        return 0;
    }
    catch (...)
    {
        return 0;
    }
}

int
chronos_acquire_references(struct chronos_client* client, uint64_t* events, size_t events_sz)
{
    try
    {
        return client->acquire_references(events, events_sz);
    }
    catch (po6::error& e)
    {
        errno = e;
        return -1;
    }
    catch (...)
    {
        return -1;
    }
}

int
chronos_release_references(struct chronos_client* client, uint64_t* events, size_t events_sz)
{
    try
    {
        return client->release_references(events, events_sz);
    }
    catch (po6::error& e)
    {
        errno = e;
        return -1;
    }
    catch (...)
    {
        return -1;
    }
}

int
chronos_query_order(struct chronos_client* client, struct chronos_pair* pairs, size_t pairs_sz)
{
    try
    {
        return client->query_order(pairs, pairs_sz);
    }
    catch (po6::error& e)
    {
        errno = e;
        return -1;
    }
    catch (...)
    {
        return -1;
    }
}

int
chronos_assign_order(struct chronos_client* client, struct chronos_pair* pairs, size_t pairs_sz)
{
    try
    {
        return client->assign_order(pairs, pairs_sz);
    }
    catch (po6::error& e)
    {
        errno = e;
        return -1;
    }
    catch (...)
    {
        return -1;
    }
}

int
chronos_get_stats(struct chronos_client* client, struct chronos_stats* st)
{
    try
    {
        return client->get_stats(st);
    }
    catch (po6::error& e)
    {
        errno = e;
        return -1;
    }
    catch (...)
    {
        return -1;
    }
}

} // extern "C"
