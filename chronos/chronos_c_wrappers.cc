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
chronos_client_create(const char* host, uint16_t port, uint64_t num_vts)
{
    try
    {
        return new chronos_client(host, port, num_vts);
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

int64_t
chronos_create_event(chronos_client* client,
                     chronos_returncode* status,
                     uint64_t* event)
{
    try
    {
        return client->create_event(status, event);
    }
    catch (po6::error& e)
    {
        *status = CHRONOS_ERROR;
        errno = e;
        return 0;
    }
    catch (...)
    {
        *status = CHRONOS_ERROR;
        return 0;
    }
}

int64_t
chronos_acquire_references(chronos_client* client,
                           uint64_t* events, size_t events_sz,
                           chronos_returncode* status, ssize_t* ret)
{
    try
    {
        return client->acquire_references(events, events_sz, status, ret);
    }
    catch (po6::error& e)
    {
        *status = CHRONOS_ERROR;
        errno = e;
        return -1;
    }
    catch (...)
    {
        *status = CHRONOS_ERROR;
        return -1;
    }
}

int64_t
chronos_release_references(chronos_client* client,
                           uint64_t* events, size_t events_sz,
                           chronos_returncode* status, ssize_t* ret)
{
    try
    {
        return client->release_references(events, events_sz, status, ret);
    }
    catch (po6::error& e)
    {
        *status = CHRONOS_ERROR;
        errno = e;
        return -1;
    }
    catch (...)
    {
        *status = CHRONOS_ERROR;
        return -1;
    }
}

int64_t
chronos_query_order(struct chronos_client* client, struct chronos_pair* pairs, size_t pairs_sz,
                    enum chronos_returncode* status, ssize_t* ret)
{
    try
    {
        return client->query_order(pairs, pairs_sz, status, ret);
    }
    catch (po6::error& e)
    {
        *status = CHRONOS_ERROR;
        errno = e;
        return -1;
    }
    catch (...)
    {
        *status = CHRONOS_ERROR;
        return -1;
    }
}

int64_t
chronos_assign_order(struct chronos_client* client, struct chronos_pair* pairs, size_t pairs_sz,
                    enum chronos_returncode* status, ssize_t* ret)
{
    try
    {
        return client->assign_order(pairs, pairs_sz, status, ret);
    }
    catch (po6::error& e)
    {
        *status = CHRONOS_ERROR;
        errno = e;
        return -1;
    }
    catch (...)
    {
        *status = CHRONOS_ERROR;
        return -1;
    }
}

int64_t
chronos_get_stats(struct chronos_client* client,
                  enum chronos_returncode* status,
                  struct chronos_stats* st,
                  ssize_t* ret)
{
    try
    {
        return client->get_stats(status, st, ret);
    }
    catch (po6::error& e)
    {
        *status = CHRONOS_ERROR;
        errno = e;
        return -1;
    }
    catch (...)
    {
        *status = CHRONOS_ERROR;
        return -1;
    }
}

int64_t
chronos_loop(struct chronos_client* client, int timeout, enum chronos_returncode* status)
{
    try
    {
        return client->loop(timeout, status);
    }
    catch (po6::error& e)
    {
        *status = CHRONOS_ERROR;
        errno = e;
        return -1;
    }
    catch (...)
    {
        *status = CHRONOS_ERROR;
        return -1;
    }
}

int64_t
chronos_wait(struct chronos_client* client, int64_t id, int timeout, enum chronos_returncode* status)
{
    try
    {
        return client->wait(id, timeout, status);
    }
    catch (po6::error& e)
    {
        *status = CHRONOS_ERROR;
        errno = e;
        return -1;
    }
    catch (...)
    {
        *status = CHRONOS_ERROR;
        return -1;
    }
}

} // extern "C"
