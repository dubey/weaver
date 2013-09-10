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
#include <e/error.h>

// Replicant
#include <replicant.h>

// Chronos
#include "chronos.h"
#include "chronos_cmp_encode.h"
#include "network_constants.h"

class chronos_client::pending
{
    public:
        pending(chronos_returncode* status);
        virtual ~pending() throw ();

    public:
        virtual void parse() = 0;

    public:
        replicant_returncode repl_status;
        const char* repl_output;
        size_t repl_output_sz;
        chronos_returncode* status;

    private:
        friend class e::intrusive_ptr<pending>;

    private:
        pending(const pending&);

    private:
        void inc() { ++m_ref; }
        void dec() { if (--m_ref == 0) delete this; }

    private:
        pending& operator = (const pending&);

    private:
        size_t m_ref;
};

class chronos_client::pending_create_event : public pending
{
    public:
        pending_create_event(chronos_returncode* status, uint64_t* event);
        virtual ~pending_create_event() throw ();

    public:
        virtual void parse();

    private:
        pending_create_event(const pending_create_event&);

    private:
        pending_create_event& operator = (const pending_create_event&);

    private:
        uint64_t* m_event;
};

class chronos_client::pending_references : public pending
{
    public:
        pending_references(chronos_returncode* status, ssize_t* ret);
        virtual ~pending_references() throw ();

    public:
        virtual void parse();

    private:
        pending_references(const pending_references&);

    private:
        pending_references& operator = (const pending_references&);

    private:
        ssize_t* m_ret;
};

class chronos_client::pending_order : public pending
{
    public:
        pending_order(chronos_returncode* status, chronos_pair* pairs, size_t sz, ssize_t* ret);
        virtual ~pending_order() throw ();

    public:
        virtual void parse();

    private:
        pending_order(const pending_order&);

    private:
        pending_order& operator = (const pending_order&);

    private:
        chronos_pair* m_pairs;
        size_t m_pairs_sz;
        ssize_t* m_ret;
};

class chronos_client::pending_weaver_order : public pending
{
    public:
        pending_weaver_order(chronos_returncode* status, weaver_pair* pairs, size_t sz, ssize_t* ret);
        virtual ~pending_weaver_order() throw ();

    public:
        virtual void parse();

    private:
        pending_weaver_order(const pending_weaver_order&);

    private:
        pending_weaver_order& operator = (const pending_weaver_order&);

    private:
        weaver_pair* m_pairs;
        size_t m_pairs_sz;
        ssize_t* m_ret;
};

class chronos_client::pending_get_stats : public pending
{
    public:
        pending_get_stats(chronos_returncode* status, chronos_stats* st, ssize_t* ret);
        virtual ~pending_get_stats() throw ();

    public:
        virtual void parse();

    private:
        pending_get_stats(const pending_get_stats&);

    private:
        pending_get_stats& operator = (const pending_get_stats&);

    private:
        chronos_stats* m_st;
        ssize_t* m_ret;
};

chronos_client :: chronos_client(const char* host, uint16_t port, uint64_t shards)
    : m_replicant(new replicant_client(host, port))
    , m_pending()
    , m_shards(shards)
{
}

chronos_client :: ~chronos_client() throw ()
{
}

int64_t
chronos_client :: create_event(chronos_returncode* status, uint64_t* event)
{
    e::intrusive_ptr<pending> pend(new pending_create_event(status, event));
    return send(pend, status, "create_event", "", 0);
}

int64_t
chronos_client :: acquire_references(uint64_t* events, size_t events_sz,
                                     chronos_returncode* status, ssize_t* ret)
{
    std::vector<char> buffer(events_sz * sizeof(uint64_t));
    char* p = &buffer.front();

    for (size_t i = 0; i < events_sz; ++i)
    {
        p = e::pack64le(events[i], p);
    }

    // We do this assignment so we can pass events_sz to the "parse" function
    *ret = events_sz;
    e::intrusive_ptr<pending> pend(new pending_references(status, ret));
    return send(pend, status, "acquire_references", &buffer.front(), buffer.size());
}

int64_t
chronos_client :: release_references(uint64_t* events, size_t events_sz,
                                     chronos_returncode* status, ssize_t* ret)
{
    std::vector<char> buffer(events_sz * sizeof(uint64_t));
    char* p = &buffer.front();

    for (size_t i = 0; i < events_sz; ++i)
    {
        p = e::pack64le(events[i], p);
    }

    e::intrusive_ptr<pending> pend(new pending_references(status, ret));
    return send(pend, status, "release_references", &buffer.front(), buffer.size());
}

int64_t
chronos_client :: query_order(chronos_pair* pairs, size_t pairs_sz,
                              chronos_returncode* status, ssize_t* ret)
{
    std::vector<char> buffer(pairs_sz * (sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t)));
    char* p = &buffer.front();

    for (size_t i = 0; i < pairs_sz; ++i)
    {
        p = e::pack64le(pairs[i].lhs, p);
        p = e::pack64le(pairs[i].rhs, p);
        p = e::pack32le(pairs[i].flags, p);
    }

    e::intrusive_ptr<pending> pend(new pending_order(status, pairs, pairs_sz, ret));
    return send(pend, status, "query_order", &buffer.front(), buffer.size());
}

int64_t
chronos_client :: assign_order(chronos_pair* pairs, size_t pairs_sz,
                               chronos_returncode* status, ssize_t* ret)
{
    std::vector<char> buffer(pairs_sz * (sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint8_t)));
    char* p = &buffer.front();

    for (size_t i = 0; i < pairs_sz; ++i)
    {
        p = e::pack64le(pairs[i].lhs, p);
        p = e::pack64le(pairs[i].rhs, p);
        p = e::pack32le(pairs[i].flags, p);
        *p = chronos_cmp_to_byte(pairs[i].order);
        ++p;
    }

    e::intrusive_ptr<pending> pend(new pending_order(status, pairs, pairs_sz, ret));
    return send(pend, status, "assign_order", &buffer.front(), buffer.size());
}

char*
pack_vector_uint64(uint64_t *vec, uint64_t vec_size, char *p)
{
    for (size_t i = 0; i < vec_size; i++) {
       p = e::pack64le(vec[i], p);
    }
    return p;
}

int64_t
chronos_client :: weaver_order(weaver_pair *pairs, size_t pairs_sz,
        chronos_returncode *status, ssize_t *ret)
{
    std::vector<char> buffer(pairs_sz * (2 * sizeof(uint64_t) * m_shards // vector clocks
            + sizeof(uint32_t) // flags
            + sizeof(uint8_t))); // preferred order
    char *p = &buffer.front();

    // pack weaver pairs
    for (size_t i = 0; i < pairs_sz; i++) {
        p = pack_vector_uint64(pairs[i].lhs, m_shards, p);
        p = pack_vector_uint64(pairs[i].rhs, m_shards, p);
        p = e::pack32le(pairs[i].flags, p);
        *p = chronos_cmp_to_byte(pairs[i].order);
        ++p;
    }

    e::intrusive_ptr<pending> pend(new pending_weaver_order(status, pairs, pairs_sz, ret));
    return send(pend, status, "weaver_order", &buffer.front(), buffer.size());
}

int64_t
chronos_client :: get_stats(chronos_returncode* status, chronos_stats* st, ssize_t* ret)
{
    e::intrusive_ptr<pending> pend(new pending_get_stats(status, st, ret));
    return send(pend, status, "get_stats", "", 0);
}

int64_t
chronos_client :: loop(int timeout, chronos_returncode* status)
{
    replicant_returncode rc;
    int64_t ret = m_replicant->loop(timeout, &rc);

    if (ret < 0)
    {
        *status = CHRONOS_ERROR;
        return -1;
    }
    else
    {
        pending_map::iterator it = m_pending.find(ret); 

        if (it == m_pending.end())
        {
            *status = CHRONOS_ERROR;
            return -1;
        }

        *status = CHRONOS_SUCCESS;
        e::intrusive_ptr<pending> pend = it->second;
        m_pending.erase(it);
        pend->parse();
        return ret;
    }
}

int64_t
chronos_client :: wait(int64_t id, int timeout, chronos_returncode* status)
{
    replicant_returncode rc;
    int64_t ret = m_replicant->loop(id, timeout, &rc);

    if (ret < 0)
    {
        *status = CHRONOS_ERROR;
        return -1;
    }
    else
    {
        pending_map::iterator it = m_pending.find(ret); 

        if (it == m_pending.end())
        {
            *status = CHRONOS_ERROR;
            return -1;
        }

        *status = CHRONOS_SUCCESS;
        e::intrusive_ptr<pending> pend = it->second;
        m_pending.erase(it);
        pend->parse();
        return ret;
    }
}

int64_t
chronos_client :: send(e::intrusive_ptr<pending> pend, chronos_returncode* status,
                       const char* func, const char* data, size_t data_sz)
{
    int64_t ret = m_replicant->send("chronosd",
                                    func, data, data_sz,
                                    &pend->repl_status,
                                    &pend->repl_output,
                                    &pend->repl_output_sz);

    if (ret > 0)
    {
        m_pending[ret] = pend;
        return ret;
    }
    else
    {
std::cerr << __FILE__ << ":" << __LINE__ << std::endl;
e::error send_error = m_replicant->last_error();
std::cerr << __FILE__ << ":" << __LINE__ << " " << send_error.msg();
std::cerr << __FILE__ << ":" << __LINE__ << " " << send_error.loc();
        *status = CHRONOS_ERROR;
        return ret;
    }
}

//////////////////////////////////// pending ///////////////////////////////////

chronos_client :: pending :: pending(chronos_returncode* s)
    : repl_status()
    , repl_output(NULL)
    , repl_output_sz(0)
    , status(s)
    , m_ref(0)
{
}

chronos_client :: pending :: ~pending() throw ()
{
    if (repl_output)
    {
        replicant_destroy_output(repl_output, repl_output_sz);
    }
}

///////////////////////////// pending_create_event /////////////////////////////

chronos_client :: pending_create_event :: pending_create_event(chronos_returncode* s, uint64_t* event)
    : pending(s)
    , m_event(event)
{
}

chronos_client :: pending_create_event :: ~pending_create_event() throw ()
{
}

void
chronos_client :: pending_create_event :: parse()
{
    if (repl_status != REPLICANT_SUCCESS ||
        repl_output_sz != sizeof(uint64_t))
    {
        *status = CHRONOS_ERROR;
    }
    else
    {
        *status = CHRONOS_SUCCESS;
        e::unpack64le(repl_output, m_event);
    }
}

////////////////////////////// pending_references //////////////////////////////

chronos_client :: pending_references :: pending_references(chronos_returncode* s, ssize_t* ret)
    : pending(s)
    , m_ret(ret)
{
}

chronos_client :: pending_references :: ~pending_references() throw ()
{
}

void
chronos_client :: pending_references :: parse()
{
    if (repl_status != REPLICANT_SUCCESS ||
        repl_output_sz != sizeof(uint64_t))
    {
        *status = CHRONOS_ERROR;
        *m_ret = -1;
    }
    else
    {
        *status = CHRONOS_SUCCESS;
        uint64_t num;
        e::unpack64le(repl_output, &num);
        *m_ret = num;
    }
}

////////////////////////////// pending_order /////////////////////////////

chronos_client :: pending_order :: pending_order(chronos_returncode* s, chronos_pair* pairs, size_t sz, ssize_t* ret)
    : pending(s)
    , m_pairs(pairs)
    , m_pairs_sz(sz)
    , m_ret(ret)
{
}

chronos_client :: pending_order :: ~pending_order() throw ()
{
}

void
chronos_client :: pending_order :: parse()
{
    if (repl_status != REPLICANT_SUCCESS ||
        repl_output_sz != m_pairs_sz)
    {
        *status = CHRONOS_ERROR;
        *m_ret = -1;
    }
    else
    {
        *status = CHRONOS_SUCCESS;
        *m_ret = repl_output_sz;

        for (size_t i = 0; i < m_pairs_sz; ++i)
        {
            m_pairs[i].order = byte_to_chronos_cmp(repl_output[i]);
        }
    }
}

////////////////////////////// pending_weaver_order /////////////////////////////

chronos_client :: pending_weaver_order :: pending_weaver_order(chronos_returncode* s, weaver_pair* pairs, size_t sz, ssize_t* ret)
    : pending(s)
    , m_pairs(pairs)
    , m_pairs_sz(sz)
    , m_ret(ret)
{
}

chronos_client :: pending_weaver_order :: ~pending_weaver_order() throw ()
{
}

void
chronos_client :: pending_weaver_order :: parse()
{
    if (repl_status != REPLICANT_SUCCESS ||
        repl_output_sz != m_pairs_sz)
    {
        *status = CHRONOS_ERROR;
        *m_ret = -1;
    }
    else
    {
        *status = CHRONOS_SUCCESS;
        *m_ret = repl_output_sz;

        for (size_t i = 0; i < m_pairs_sz; ++i)
        {
            m_pairs[i].order = byte_to_chronos_cmp(repl_output[i]);
        }
    }
}

/////////////////////////////// pending_get_stats //////////////////////////////

chronos_client :: pending_get_stats :: pending_get_stats(chronos_returncode* s, chronos_stats* st, ssize_t* ret)
    : pending(s)
    , m_st(st)
    , m_ret(ret)
{
}

chronos_client :: pending_get_stats :: ~pending_get_stats() throw ()
{
}

void
chronos_client :: pending_get_stats :: parse()
{
    if (repl_status != REPLICANT_SUCCESS ||
        repl_output_sz != sizeof(uint64_t) * 9 + sizeof(uint32_t))
    {
        *status = CHRONOS_ERROR;
        *m_ret = -1;
    }
    else
    {
        *status = CHRONOS_SUCCESS;
        *m_ret = 0;
        const char* c = repl_output;
        c = e::unpack64le(c, &m_st->time);
        c = e::unpack64le(c, &m_st->utime);
        c = e::unpack64le(c, &m_st->stime);
        c = e::unpack32le(c, &m_st->maxrss);
        c = e::unpack64le(c, &m_st->events);
        c = e::unpack64le(c, &m_st->count_create_event);
        c = e::unpack64le(c, &m_st->count_acquire_references);
        c = e::unpack64le(c, &m_st->count_release_references);
        c = e::unpack64le(c, &m_st->count_query_order);
        c = e::unpack64le(c, &m_st->count_assign_order);
        c = e::unpack64le(c, &m_st->count_weaver_order);
    }
}
