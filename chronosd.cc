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

// POSIX
#include <sys/time.h>
#include <sys/resource.h>

#include <iostream>
#include <vector>
#include <set>
#include <unordered_map>

// e
#include <e/endian.h>

// Replicant
#include <replicant_state_machine.h>

// Chronos
#include "chronos.h"
#include "chronos_cmp_encode.h"
#include "event_dependency_graph.h"

#define xtostr(X) #X
#define tostr(X) xtostr(X)
#define ERRORMSG(X) fprintf(stderr, "%s:%i:  " X "\n", __FILE__, __LINE__)
#define ERRORMSG1(X, Y) fprintf(stderr, "%s:%i:  " X "\n", __FILE__, __LINE__, Y)
#define ERRORMSG2(X, Y1, Y2) fprintf(stderr, "%s:%i:  " X "\n", __FILE__, __LINE__, Y1, Y2)
#define ERRNOMSG(CALL) ERRORMSG2(tostr(CALL) " failed:  %s  [ERRNO=%i]", strerror(errno), errno)

// hash function for uint64
size_t
hash_uint64(uint64_t key)
{
    return std::hash<uint64_t>()(key);
}

// hash function for vector clocks
namespace std
{
    template <>
    struct hash<std::vector<uint64_t>> 
    {
        public:
            size_t operator()(std::vector<uint64_t> v) const throw() 
            {
                size_t hash = hash_uint64(v[0]);
                for (size_t i = 1; i < v.size(); i++) {
                    hash = hash ^ v[i];
                    hash = hash_uint64(hash);
                }
                return hash;
            }
    };
}

// comparator for uint64 pair, on the basis of first entry of the pair
bool
pair_comp(std::pair<uint64_t, uint64_t> p1, std::pair<uint64_t, uint64_t> p2)
{
    return p1.first < p2.first;
}


class chronosd
{
    public:
        chronosd();
        ~chronosd() throw ();

    public:
        void create_event(struct replicant_state_machine_context* ctx,
                          const char* data, size_t data_sz);
        void acquire_references(struct replicant_state_machine_context* ctx,
                                const char* data, size_t data_sz);
        void release_references(struct replicant_state_machine_context* ctx,
                                const char* data, size_t data_sz);
        void query_order(struct replicant_state_machine_context* ctx,
                         const char* data, size_t data_sz);
        void assign_order(struct replicant_state_machine_context* ctx,
                          const char* data, size_t data_sz);
        void weaver_order(struct replicant_state_machine_context* ctx,
                          const char* data, size_t data_sz);
        void get_stats(struct replicant_state_machine_context* ctx,
                       const char* data, size_t data_sz);

    private:
        event_dependency_graph m_graph;
        uint64_t m_count_create_event;
        uint64_t m_count_acquire_references;
        uint64_t m_count_release_references;
        uint64_t m_count_query_order;
        uint64_t m_count_assign_order;
        char *m_repl_resp; // buffer for setting replicant response

    // Weaver
    public:
        typedef std::set<std::pair<uint64_t, uint64_t>,
                bool(*)(std::pair<uint64_t, uint64_t>, std::pair<uint64_t, uint64_t>)> pair_set_t;
    private:
        uint64_t m_count_weaver_order;
        std::unordered_map<std::vector<uint64_t>, uint64_t> m_vcmap; // vclk -> kronos id
        bool (*pair_comp_ptr)(std::pair<uint64_t, uint64_t>, std::pair<uint64_t, uint64_t>);
        std::unordered_map<uint64_t, pair_set_t> m_vtlist; // vt id -> (vclk, corresponding kronos id) seen from that vt
        void assign_vt_dependencies(std::vector<uint64_t> &vclk, uint64_t vt_id);
};

chronosd :: chronosd()
    : m_graph()
    , m_count_create_event()
    , m_count_acquire_references()
    , m_count_release_references()
    , m_count_query_order()
    , m_count_assign_order()
    , m_count_weaver_order()
    , m_repl_resp(NULL)
    , pair_comp_ptr(&pair_comp)
{
    pair_set_t empty_set(pair_comp_ptr);
    for (uint64_t vt_id = 0; vt_id < KRONOS_NUM_VTS; vt_id++) {
        m_vtlist.emplace(vt_id, empty_set);
    }
}

chronosd :: ~chronosd() throw ()
{
}

void
chronosd :: create_event(struct replicant_state_machine_context* ctx,
                         const char*, size_t)
{
    ++m_count_create_event;
    uint64_t event = m_graph.add_vertex();
    if (m_repl_resp) {
        free(m_repl_resp);
    }
    m_repl_resp = (char*)malloc(sizeof(uint64_t));
    e::pack64le(event, m_repl_resp);
    replicant_state_machine_set_response(ctx, m_repl_resp, sizeof(m_repl_resp));
}

void
chronosd :: acquire_references(struct replicant_state_machine_context* ctx,
                               const char* data, size_t data_sz)
{
    ++m_count_acquire_references;
    const size_t NUM_EVENTS = data_sz / sizeof(uint64_t);
    std::vector<uint64_t> events;
    std::vector<uint64_t> event_offsets;
    events.reserve(NUM_EVENTS);
    const char* c = data;

    // Capture only those events that need a refcount; skip those we already
    // refer to
    for (size_t i = 0; i < NUM_EVENTS; ++i)
    {
        uint64_t e;
        c = e::unpack64le(c, &e);

        // XXX if not already referenced
        {
            events.push_back(e);
            event_offsets.push_back(i);
        }
    }

    uint64_t response = NUM_EVENTS;
    size_t num_events = 0;

    // In the normal case, we'll process all events
    for (num_events = 0; num_events < events.size(); ++num_events)
    {
        if (!m_graph.incref(events[num_events]))
        {
            break;
        }
    }

    // If we don't, we need to rever those we already incref'd and then set the
    // response to reflect that we only got so far.
    if (num_events != events.size())
    {
        for (size_t j = 0; j < num_events; ++j)
        {
            bool decr = m_graph.decref(events[num_events]);
            assert(decr);
        }

        response = event_offsets[num_events];
    }

    // Write the response
    if (m_repl_resp) {
        free(m_repl_resp);
    }
    m_repl_resp = (char*)malloc(sizeof(uint64_t));
    e::pack64le(response, m_repl_resp);
    replicant_state_machine_set_response(ctx, m_repl_resp, sizeof(m_repl_resp));

    if (response == NUM_EVENTS)
    {
        // XXX need to add a reference associated with the channel
    }
}

void
chronosd :: release_references(struct replicant_state_machine_context* ctx,
                               const char* data, size_t data_sz)
{
    ++m_count_release_references;
    const size_t NUM_EVENTS = data_sz / sizeof(uint64_t);
    const char* c = data;

    // Capture only those events that need a refcount; skip those we already
    // refer to
    for (size_t i = 0; i < NUM_EVENTS; ++i)
    {
        uint64_t e;
        c = e::unpack64le(c, &e);

        // XXX if already referenced
        {
            m_graph.decref(e);
            // XXX need to remove a reference associated with the channel
        }
    }

    // Write the response
    uint64_t response = NUM_EVENTS;
    if (m_repl_resp) {
        free(m_repl_resp);
    }
    m_repl_resp = (char*)malloc(sizeof(uint64_t));
    e::pack64le(response, m_repl_resp);
    replicant_state_machine_set_response(ctx, m_repl_resp, sizeof(m_repl_resp));
}

void
chronosd :: query_order(struct replicant_state_machine_context* ctx,
                        const char* data, size_t data_sz)
{
    ++m_count_query_order;
    const size_t NUM_PAIRS = data_sz / (2 * sizeof(uint64_t) + sizeof(uint32_t));
    uint8_t *response = (uint8_t*)malloc(NUM_PAIRS * sizeof(uint8_t));
    const char* c = data;

    for (size_t i = 0; i < NUM_PAIRS; ++i)
    {
        chronos_pair p;
        c = e::unpack64le(c, &p.lhs);
        c = e::unpack64le(c, &p.rhs);
        c = e::unpack32le(c, &p.flags);
        p.order = CHRONOS_CONCURRENT;

        if (!m_graph.exists(p.lhs) ||
            !m_graph.exists(p.rhs))
        {
            p.order = CHRONOS_NOEXIST;
        }
        else
        {
            int resolve = m_graph.compute_order(p.lhs, p.rhs);

            if (resolve < 0)
            {
                p.order = CHRONOS_HAPPENS_BEFORE;
            }
            else if (resolve > 0)
            {
                p.order = CHRONOS_HAPPENS_AFTER;
            }
            else
            {
                p.order = CHRONOS_CONCURRENT;
            }
        }

        response[i] = chronos_cmp_to_byte(p.order);
    }

    if (m_repl_resp) {
        free(m_repl_resp);
    }
    m_repl_resp = (char*)response;
    replicant_state_machine_set_response(ctx, m_repl_resp, NUM_PAIRS * sizeof(uint8_t));
}

void
chronosd :: assign_order(struct replicant_state_machine_context* ctx,
                         const char* data, size_t data_sz)
{
    ++m_count_assign_order;
    const size_t NUM_PAIRS = data_sz / (2 * sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint8_t));
    uint8_t *results = (uint8_t*)malloc(NUM_PAIRS * sizeof(uint8_t));
    for (size_t i = 0; i < NUM_PAIRS; i++) {
        results[i] = chronos_cmp_to_byte(CHRONOS_WOULDLOOP);
    }
    std::vector<std::pair<uint64_t, uint64_t> > edges;
    edges.reserve(NUM_PAIRS);
    size_t num_pairs = 0;
    const char* c = data;

    for (num_pairs = 0; num_pairs < NUM_PAIRS; ++num_pairs)
    {
        chronos_pair p;
        uint8_t o;
        c = e::unpack64le(c, &p.lhs);
        c = e::unpack64le(c, &p.rhs);
        c = e::unpack32le(c, &p.flags);
        c = e::unpack8le(c, &o);
        p.order = byte_to_chronos_cmp(o);

        if (!m_graph.exists(p.lhs) ||
            !m_graph.exists(p.rhs))
        {
            results[num_pairs] = chronos_cmp_to_byte(CHRONOS_NOEXIST);
            break;
        }

        int resolve = m_graph.compute_order(p.lhs, p.rhs);

        if (resolve < 0)
        {
            results[num_pairs] = chronos_cmp_to_byte(CHRONOS_HAPPENS_BEFORE);

            if (p.order != CHRONOS_HAPPENS_BEFORE)
            {
                if (!(p.flags & CHRONOS_SOFT_FAIL))
                {
                    results[num_pairs] = chronos_cmp_to_byte(CHRONOS_WOULDLOOP);
                    break;
                }
            }
        }
        else if (resolve > 0)
        {
            results[num_pairs] = chronos_cmp_to_byte(CHRONOS_HAPPENS_AFTER);

            if (p.order != CHRONOS_HAPPENS_AFTER)
            {
                if (!(p.flags & CHRONOS_SOFT_FAIL))
                {
                    results[num_pairs] = chronos_cmp_to_byte(CHRONOS_WOULDLOOP);
                    break;
                }
            }
        }
        else
        {
            switch (p.order)
            {
                case CHRONOS_HAPPENS_BEFORE:
                    results[num_pairs] = chronos_cmp_to_byte(CHRONOS_HAPPENS_BEFORE);
                    m_graph.add_edge(p.lhs, p.rhs);
                    edges.push_back(std::make_pair(p.lhs, p.rhs));
                    break;
                case CHRONOS_HAPPENS_AFTER:
                    results[num_pairs] = chronos_cmp_to_byte(CHRONOS_HAPPENS_AFTER);
                    m_graph.add_edge(p.rhs, p.lhs);
                    edges.push_back(std::make_pair(p.rhs, p.lhs));
                    break;
                case CHRONOS_CONCURRENT:
                case CHRONOS_WOULDLOOP:
                case CHRONOS_NOEXIST:
                default:
                    break;
            }
        }
    }

    if (num_pairs != NUM_PAIRS)
    {
        for (size_t i = 0; i < edges.size(); ++i)
        {
            m_graph.remove_edge(edges[i].first, edges[i].second);
        }
    }

    if (m_repl_resp) {
        free(m_repl_resp);
    }
    m_repl_resp = (char*)results;
    replicant_state_machine_set_response(ctx, m_repl_resp, NUM_PAIRS * sizeof(uint8_t));
}

// this method makes edges in the event dependency graph to record
// dependencies between events from the same vector timestamper
void
chronosd :: assign_vt_dependencies(std::vector<uint64_t> &vclk, uint64_t vt_id)
{
    uint64_t clk_val = vclk.at(vt_id);
    uint64_t ev_id = m_vcmap[vclk];
    assert(m_vtlist.find(vt_id) != m_vtlist.end());
    pair_set_t &vtlist = m_vtlist[vt_id];
    auto res = vtlist.emplace(std::make_pair(clk_val, ev_id));
    assert(res.second);
    auto iter = res.first;
    // make fwd edge
    iter++; // iter is now element succeeding newly inserted element
    if (iter != vtlist.end()) {
        m_graph.add_edge(ev_id, iter->second);
    }
    iter--;
    // make reverse edge
    if (iter != vtlist.begin()) {
        iter--; // iter is now element preceding newly inserted element
        m_graph.add_edge(iter->second, ev_id);
    }
}

const char*
unpack_vector_uint64(const char *c, uint64_t **vec, uint64_t vec_size)
{
    *vec = (uint64_t*)malloc(sizeof(uint64_t) * vec_size);
    for (size_t i = 0; i < vec_size; i++) {
        c = e::unpack64le(c, *vec + i);
    }
    return c;
}

void
chronosd :: weaver_order(struct replicant_state_machine_context* ctx,
                         const char* data, size_t data_sz)
{
    ++m_count_weaver_order;
    const size_t NUM_PAIRS = data_sz / (2 * sizeof(uint64_t) * KRONOS_NUM_VTS // vector clocks
            + 2 * sizeof(uint64_t) // vt_ids
            + sizeof(uint32_t) // flags
            + sizeof(uint8_t)); // preferred order
    uint8_t *results = (uint8_t*)malloc(NUM_PAIRS * sizeof(uint8_t));
    for (size_t i = 0; i < NUM_PAIRS; i++) {
        results[i] = chronos_cmp_to_byte(CHRONOS_WOULDLOOP);
    }
    size_t num_pairs = 0;
    const char* c = data;

    for (num_pairs = 0; num_pairs < NUM_PAIRS; ++num_pairs) {
        chronos_pair p;
        weaver_pair wp;
        uint8_t o;
        c = unpack_vector_uint64(c, &wp.lhs, KRONOS_NUM_VTS);
        c = unpack_vector_uint64(c, &wp.rhs, KRONOS_NUM_VTS);
        c = e::unpack64le(c, &wp.lhs_id);
        c = e::unpack64le(c, &wp.rhs_id);
        c = e::unpack32le(c, &p.flags);
        c = e::unpack8le(c, &o);
        p.order = byte_to_chronos_cmp(o);

        // Bunch of sanity checks for Weaver provided vector clocks
        // some order should have been provided
        assert((p.order == CHRONOS_HAPPENS_BEFORE) || (p.order == CHRONOS_HAPPENS_AFTER));
        // CHRONOS_SOFT_FAIL should have been enabled
        assert(p.flags & CHRONOS_SOFT_FAIL);

        std::vector<uint64_t> vc_lhs, vc_rhs;
        vc_lhs.reserve(KRONOS_NUM_VTS);
        vc_rhs.reserve(KRONOS_NUM_VTS);
        for (size_t i = 0; i < KRONOS_NUM_VTS; i++) {
            vc_lhs.push_back(wp.lhs[i]);
            vc_rhs.push_back(wp.rhs[i]);
        }
        if (m_vcmap.find(vc_lhs) == m_vcmap.end()) {
            uint64_t ev_lhs = m_graph.add_vertex();
            m_vcmap[vc_lhs] = ev_lhs;
            assign_vt_dependencies(vc_lhs, wp.lhs_id);
        }
        if (m_vcmap.find(vc_rhs) == m_vcmap.end()) {
            uint64_t ev_rhs = m_graph.add_vertex();
            m_vcmap[vc_rhs] = ev_rhs;
            assign_vt_dependencies(vc_rhs, wp.rhs_id);
        }
        p.lhs = m_vcmap[vc_lhs];
        p.rhs = m_vcmap[vc_rhs];

        assert(m_graph.exists(p.lhs) && m_graph.exists(p.rhs));

        int resolve = m_graph.compute_order(p.lhs, p.rhs);

        if (resolve < 0) {
            results[num_pairs] = chronos_cmp_to_byte(CHRONOS_HAPPENS_BEFORE);
        } else if (resolve > 0) {
            results[num_pairs] = chronos_cmp_to_byte(CHRONOS_HAPPENS_AFTER);
        } else {
            switch (p.order) {
                case CHRONOS_HAPPENS_BEFORE:
                    results[num_pairs] = chronos_cmp_to_byte(CHRONOS_HAPPENS_BEFORE);
                    m_graph.add_edge(p.lhs, p.rhs);
                    break;

                case CHRONOS_HAPPENS_AFTER:
                    results[num_pairs] = chronos_cmp_to_byte(CHRONOS_HAPPENS_AFTER);
                    m_graph.add_edge(p.rhs, p.lhs);
                    break;

                default:
                    KDEBUG << "should not reach here" << std::endl;
                    assert(false);
                    break;
            }
        }
    }

    assert(num_pairs == NUM_PAIRS);

    if (m_repl_resp) {
        free(m_repl_resp);
    }
    m_repl_resp = (char*)results;
    replicant_state_machine_set_response(ctx, m_repl_resp, NUM_PAIRS * sizeof(uint8_t));
}

void
chronosd :: get_stats(struct replicant_state_machine_context* ctx,
                      const char*, size_t)
{
    chronos_stats st;
    timespec t;

    if (clock_gettime(CLOCK_REALTIME, &t) < 0)
    {
        ERRNOMSG(clock_gettime);
        st.time = 0;
    }
    else
    {
        st.time = t.tv_sec * 1000000000;
        st.time += t.tv_nsec;
    }

    rusage r;

    if (getrusage(RUSAGE_SELF, &r) < 0)
    {
        ERRNOMSG(getrusage);
        st.utime = 0;
        st.stime = 0;
        st.maxrss = 0;
    }
    else
    {
        st.utime = r.ru_utime.tv_sec * 1000000000;
        st.utime += r.ru_utime.tv_usec * 1000;
        st.stime = r.ru_stime.tv_sec * 1000000000;
        st.stime += r.ru_stime.tv_usec * 1000;
        st.maxrss = r.ru_maxrss;
    }

    st.events = m_graph.num_vertices();
    st.count_create_event = m_count_create_event;
    st.count_acquire_references = m_count_acquire_references;
    st.count_release_references = m_count_release_references;
    st.count_query_order = m_count_query_order;
    st.count_assign_order = m_count_assign_order;
    st.count_weaver_order = m_count_weaver_order;

    char *buf = (char*)malloc(sizeof(uint64_t) * 9 + sizeof(uint32_t));
    char* c = buf;
    c = e::pack64le(st.time, c);
    c = e::pack64le(st.utime, c);
    c = e::pack64le(st.stime, c);
    c = e::pack32le(st.maxrss, c);
    c = e::pack64le(st.events, c);
    c = e::pack64le(st.count_create_event, c);
    c = e::pack64le(st.count_acquire_references, c);
    c = e::pack64le(st.count_release_references, c);
    c = e::pack64le(st.count_query_order, c);
    c = e::pack64le(st.count_assign_order, c);
    c = e::pack64le(st.count_weaver_order, c);
    replicant_state_machine_set_response(ctx, buf, sizeof(buf));
}

extern "C"
{

void*
chronosd_create(struct replicant_state_machine_context*)
{
    return new (std::nothrow) chronosd();
}

void*
chronosd_recreate(struct replicant_state_machine_context* ctx,
                  const char*, size_t)
{
    // XXX
    FILE* log = replicant_state_machine_log_stream(ctx);
    fprintf(log, "chronosd does not recreate from snapshots");
    abort();
}

void
chronosd_destroy(struct replicant_state_machine_context*, void* f)
{
    if (f)
    {
        delete static_cast<chronosd*>(f);
    }
}

void
chronosd_snapshot(struct replicant_state_machine_context* ctx,
                  void*, const char** data, size_t* sz)
{
    // XXX
    FILE* log = replicant_state_machine_log_stream(ctx);
    fprintf(log, "chronosd does not take snapshots");
    *data = NULL;
    *sz = 0;
}

void
chronosd_create_event(struct replicant_state_machine_context* ctx, void* obj,
                          const char* data, size_t data_sz)
{
    static_cast<chronosd*>(obj)->create_event(ctx, data, data_sz);
}

void
chronosd_acquire_references(struct replicant_state_machine_context* ctx, void* obj,
                                const char* data, size_t data_sz)
{
    static_cast<chronosd*>(obj)->acquire_references(ctx, data, data_sz);
}

void
chronosd_release_references(struct replicant_state_machine_context* ctx, void* obj,
                                const char* data, size_t data_sz)
{
    static_cast<chronosd*>(obj)->release_references(ctx, data, data_sz);
}

void
chronosd_query_order(struct replicant_state_machine_context* ctx, void* obj,
                         const char* data, size_t data_sz)
{
    static_cast<chronosd*>(obj)->query_order(ctx, data, data_sz);
}

void
chronosd_assign_order(struct replicant_state_machine_context* ctx, void* obj,
                          const char* data, size_t data_sz)
{
    static_cast<chronosd*>(obj)->assign_order(ctx, data, data_sz);
}

void
chronosd_weaver_order(struct replicant_state_machine_context* ctx, void* obj,
                          const char* data, size_t data_sz)
{
    static_cast<chronosd*>(obj)->weaver_order(ctx, data, data_sz);
}

void
chronosd_get_stats(struct replicant_state_machine_context* ctx, void* obj,
                       const char* data, size_t data_sz)
{
    static_cast<chronosd*>(obj)->get_stats(ctx, data, data_sz);
}

} // extern "C"
