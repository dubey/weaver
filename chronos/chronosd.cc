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

// others
#include <yaml.h>

// e
#include <e/endian.h>
#include <e/popt.h>
#include <e/buffer.h>

// Replicant
#include <rsm.h>

// Chronos
#include "common/weaver_constants.h"
#include "common/config_constants.h"
#include "common/stl_serialization.h"
#include "chronos/chronos.h"
#include "chronos/chronos_cmp_encode.h"
#include "chronos/event_dependency_graph.h"

#define xtostr(X) #X
#define tostr(X) xtostr(X)
#define ERRORMSG(X) fprintf(stderr, "%s:%i:  " X "\n", __FILE__, __LINE__)
#define ERRORMSG1(X, Y) fprintf(stderr, "%s:%i:  " X "\n", __FILE__, __LINE__, Y)
#define ERRORMSG2(X, Y1, Y2) fprintf(stderr, "%s:%i:  " X "\n", __FILE__, __LINE__, Y1, Y2)
#define ERRNOMSG(CALL) ERRORMSG2(tostr(CALL) " failed:  %s  [ERRNO=%i]", strerror(errno), errno)

DECLARE_CONFIG_CONSTANTS;

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
        void create_event(struct rsm_context* ctx,
                          const char* data, size_t data_sz);
        void acquire_references(struct rsm_context* ctx,
                                const char* data, size_t data_sz);
        void release_references(struct rsm_context* ctx,
                                const char* data, size_t data_sz);
        void query_order(struct rsm_context* ctx,
                         const char* data, size_t data_sz);
        void assign_order(struct rsm_context* ctx,
                          const char* data, size_t data_sz);
        void weaver_order(struct rsm_context* ctx,
                          const char* data, size_t data_sz);
        void weaver_cleanup(struct rsm_context* ctx,
                            const char* data, size_t data_sz);
        void get_stats(struct rsm_context* ctx,
                       const char* data, size_t data_sz);
        void new_epoch(struct rsm_context* ctx,
                       const char* data, size_t data_sz);

    // backup / restore
    public:
        static chronosd* recreate(rsm_context *ctx,
                                  const char *data, size_t data_sz);
        int snapshot(rsm_context *ctx,
                     char **data, size_t *data_sz);
    private:
        event_dependency_graph m_graph;
        uint64_t m_count_create_event;
        uint64_t m_count_acquire_references;
        uint64_t m_count_release_references;
        uint64_t m_count_query_order;
        uint64_t m_count_assign_order;
        std::vector<char> m_repl_resp; // buffer for setting replicant response

    // Weaver
    public:
        using pair_set_t = std::set<std::pair<uint64_t, uint64_t>, bool(*)(std::pair<uint64_t, uint64_t>, std::pair<uint64_t, uint64_t>)>;
    private:
        uint64_t m_count_weaver_order;
        uint64_t m_count_weaver_cleanup;
        std::unordered_map<std::vector<uint64_t>, uint64_t> m_vcmap; // vclk -> kronos id
        std::unordered_map<uint64_t, std::vector<uint64_t>> m_rev_vcmap; // kronos id -> vclk
        bool (*pair_comp_ptr)(std::pair<uint64_t, uint64_t>, std::pair<uint64_t, uint64_t>);
        std::vector<std::vector<pair_set_t>> m_vtlist; // epoch num -> vt id -> (vclk, corresponding kronos id) seen from that vt
        std::vector<std::vector<uint64_t>> m_cleanup_clk; // vt id -> last cleanup clk heard from that vt
        void assign_vt_dependencies(std::vector<uint64_t> &vclk, uint64_t vt_id, uint64_t ev_id);
        void gc_weaver_event(uint64_t event_id);
};

chronosd :: chronosd()
    : m_graph()
    , m_count_create_event(0)
    , m_count_acquire_references(0)
    , m_count_release_references(0)
    , m_count_query_order(0)
    , m_count_assign_order(0)
    , m_count_weaver_order(0)
    , m_count_weaver_cleanup(0)
    , pair_comp_ptr(&pair_comp)
{
    // default search paths for config file
    if (!init_config_constants()) {
        abort();
    }

    m_vtlist.emplace_back(std::vector<pair_set_t>()); // epoch num 0

    pair_set_t empty_set(pair_comp_ptr);
    m_vtlist[0].reserve(NumVts);
    std::vector<uint64_t> empty_clk(ClkSz, 0);
    for (uint64_t vt_id = 0; vt_id < NumVts; vt_id++) {
        m_vtlist[0].emplace_back(empty_set);
        m_cleanup_clk.emplace_back(empty_clk);
    }
}

chronosd :: ~chronosd() throw ()
{
}

void
chronosd :: create_event(struct rsm_context* ctx,
                         const char*, size_t)
{
    ++m_count_create_event;
    uint64_t event = m_graph.add_vertex();
    const size_t resp_sz = sizeof(uint64_t);
    m_repl_resp.resize(resp_sz);
    char *resp_ptr = &m_repl_resp.front();
    e::pack64le(event, resp_ptr);
    rsm_set_output(ctx, resp_ptr, resp_sz);
}

void
chronosd :: acquire_references(struct rsm_context* ctx,
                               const char* data, size_t data_sz)
{
    ++m_count_acquire_references;
    const size_t NUM_EVENTS = data_sz / sizeof(uint64_t);
    std::vector<uint64_t> events;
    std::vector<uint64_t> event_offsets;
    events.reserve(NUM_EVENTS);
    const char *data_ptr = data;

    // Capture only those events that need a refcount; skip those we already
    // refer to
    for (size_t i = 0; i < NUM_EVENTS; ++i)
    {
        uint64_t e;
        data_ptr = e::unpack64le(data_ptr, &e);

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
    const size_t resp_sz = sizeof(uint64_t);
    m_repl_resp.resize(resp_sz);
    char *resp_ptr = &m_repl_resp.front();
    e::pack64le(response, resp_ptr);
    rsm_set_output(ctx, resp_ptr, resp_sz);

    if (response == NUM_EVENTS)
    {
        // XXX need to add a reference associated with the channel
    }
}

void
chronosd :: release_references(struct rsm_context* ctx,
                               const char* data, size_t data_sz)
{
    ++m_count_release_references;
    const size_t NUM_EVENTS = data_sz / sizeof(uint64_t);
    const char *data_ptr = data;

    // Capture only those events that need a refcount; skip those we already
    // refer to
    for (size_t i = 0; i < NUM_EVENTS; ++i)
    {
        uint64_t e;
        data_ptr = e::unpack64le(data_ptr, &e);

        // XXX if already referenced
        {
            m_graph.decref(e);
            // XXX need to remove a reference associated with the channel
        }
    }

    // Write the response
    uint64_t response = NUM_EVENTS;
    const size_t resp_sz = sizeof(uint64_t);
    m_repl_resp.resize(resp_sz);
    char *resp_ptr = &m_repl_resp.front();
    e::pack64le(response, resp_ptr);
    rsm_set_output(ctx, resp_ptr, resp_sz);
}

void
chronosd :: query_order(struct rsm_context* ctx,
                        const char* data, size_t data_sz)
{
    ++m_count_query_order;
    const size_t NUM_PAIRS = data_sz / (2 * sizeof(uint64_t) + sizeof(uint32_t));
    const size_t resp_sz = NUM_PAIRS;
    m_repl_resp.resize(resp_sz);
    const char *data_ptr = data;

    for (size_t i = 0; i < NUM_PAIRS; ++i)
    {
        chronos_pair p;
        data_ptr = e::unpack64le(data_ptr, &p.lhs);
        data_ptr = e::unpack64le(data_ptr, &p.rhs);
        data_ptr = e::unpack32le(data_ptr, &p.flags);
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

        m_repl_resp[i] = chronos_cmp_to_byte(p.order);
    }

    char *resp_ptr = &m_repl_resp.front();
    rsm_set_output(ctx, resp_ptr, resp_sz);
}

void
chronosd :: assign_order(struct rsm_context* ctx,
                         const char* data, size_t data_sz)
{
    ++m_count_assign_order;
    const size_t NUM_PAIRS = data_sz / (2 * sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint8_t));
    const size_t resp_sz = NUM_PAIRS;
    m_repl_resp.resize(resp_sz);
    for (size_t i = 0; i < NUM_PAIRS; i++) {
        m_repl_resp[i] = chronos_cmp_to_byte(CHRONOS_WOULDLOOP);
    }
    std::vector<std::pair<uint64_t, uint64_t> > edges;
    edges.reserve(NUM_PAIRS);
    size_t num_pairs = 0;
    const char *data_ptr = data;

    for (num_pairs = 0; num_pairs < NUM_PAIRS; ++num_pairs)
    {
        chronos_pair p;
        uint8_t o;
        data_ptr = e::unpack64le(data_ptr, &p.lhs);
        data_ptr = e::unpack64le(data_ptr, &p.rhs);
        data_ptr = e::unpack32le(data_ptr, &p.flags);
        data_ptr = e::unpack8le(data_ptr, &o);
        p.order = byte_to_chronos_cmp(o);

        if (!m_graph.exists(p.lhs) ||
            !m_graph.exists(p.rhs))
        {
            m_repl_resp[num_pairs] = chronos_cmp_to_byte(CHRONOS_NOEXIST);
            break;
        }

        int resolve = m_graph.compute_order(p.lhs, p.rhs);

        if (resolve < 0)
        {
            m_repl_resp[num_pairs] = chronos_cmp_to_byte(CHRONOS_HAPPENS_BEFORE);

            if (p.order != CHRONOS_HAPPENS_BEFORE)
            {
                if (!(p.flags & CHRONOS_SOFT_FAIL))
                {
                    m_repl_resp[num_pairs] = chronos_cmp_to_byte(CHRONOS_WOULDLOOP);
                    break;
                }
            }
        }
        else if (resolve > 0)
        {
            m_repl_resp[num_pairs] = chronos_cmp_to_byte(CHRONOS_HAPPENS_AFTER);

            if (p.order != CHRONOS_HAPPENS_AFTER)
            {
                if (!(p.flags & CHRONOS_SOFT_FAIL))
                {
                    m_repl_resp[num_pairs] = chronos_cmp_to_byte(CHRONOS_WOULDLOOP);
                    break;
                }
            }
        }
        else
        {
            switch (p.order)
            {
                case CHRONOS_HAPPENS_BEFORE:
                    m_repl_resp[num_pairs] = chronos_cmp_to_byte(CHRONOS_HAPPENS_BEFORE);
                    m_graph.add_edge(p.lhs, p.rhs);
                    edges.push_back(std::make_pair(p.lhs, p.rhs));
                    break;
                case CHRONOS_HAPPENS_AFTER:
                    m_repl_resp[num_pairs] = chronos_cmp_to_byte(CHRONOS_HAPPENS_AFTER);
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

    char *resp_ptr = &m_repl_resp.front();
    rsm_set_output(ctx, resp_ptr, resp_sz);
}

// this method makes edges in the event dependency graph to record
// dependencies between events from the same vector timestamper
void
chronosd :: assign_vt_dependencies(std::vector<uint64_t> &vclk, uint64_t vt_id, uint64_t ev_id)
{
    // ev_id = m_vcmap[vclk];
    uint64_t clk_val = vclk[vt_id+1];
    uint64_t epoch = vclk[0];

    if (epoch >= m_vtlist.size()) {
        pair_set_t empty_set(pair_comp_ptr);
        std::vector<pair_set_t> initializer(NumVts, empty_set);
        m_vtlist.resize(epoch+1, initializer);
    }

    assert(vt_id < NumVts && m_vtlist[epoch].size() == NumVts);
    pair_set_t &vtlist = m_vtlist[epoch][vt_id];
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
unpack_vector_uint64(const char *c, std::vector<uint64_t> &vec, uint64_t vec_size)
{
    vec.resize(vec_size, 0);
    for (size_t i = 0; i < vec_size; i++) {
        c = e::unpack64le(c, &vec[i]);
    }
    return c;
}

void
chronosd :: weaver_order(struct rsm_context* ctx,
                         const char* data, size_t data_sz)
{
    ++m_count_weaver_order;
    const size_t NUM_PAIRS = data_sz / (2 * sizeof(uint64_t) * ClkSz // vector clocks
            + 2 * sizeof(uint64_t) // vt_ids
            + sizeof(uint32_t) // flags
            + sizeof(uint8_t)); // preferred order
    const size_t resp_sz = NUM_PAIRS;
    m_repl_resp.resize(resp_sz);
    for (size_t i = 0; i < NUM_PAIRS; i++) {
        m_repl_resp[i] = chronos_cmp_to_byte(CHRONOS_WOULDLOOP);
    }

    std::vector<std::pair<uint64_t, uint64_t>> edges;
    edges.reserve(NUM_PAIRS);
    size_t num_pairs = 0;
    const char *data_ptr = data;

    for (num_pairs = 0; num_pairs < NUM_PAIRS; ++num_pairs) {
        // unpack weaver pair
        chronos_pair p;
        weaver_pair wp;
        uint8_t o;
        data_ptr = unpack_vector_uint64(data_ptr, wp.lhs, ClkSz);
        data_ptr = unpack_vector_uint64(data_ptr, wp.rhs, ClkSz);
        data_ptr = e::unpack64le(data_ptr, &wp.lhs_id);
        data_ptr = e::unpack64le(data_ptr, &wp.rhs_id);
        data_ptr = e::unpack32le(data_ptr, &p.flags);
        data_ptr = e::unpack8le(data_ptr, &o);
        p.order = byte_to_chronos_cmp(o);

        // Bunch of sanity checks for Weaver provided vector clocks
        // some order should have been provided
        assert((p.order == CHRONOS_HAPPENS_BEFORE) || (p.order == CHRONOS_HAPPENS_AFTER));
        // we need SOFT_FAIL only for query weaver_order.  For kronos calls
        // from timestamper it is hard fail
        //assert(p.flags & CHRONOS_SOFT_FAIL);

        std::vector<uint64_t> &vc_lhs = wp.lhs;
        std::vector<uint64_t> &vc_rhs = wp.rhs;
        // create vertex in dependency graph if doesn't exist
        auto iter = m_vcmap.find(vc_lhs);
        if (iter == m_vcmap.end()) {
            uint64_t ev_lhs = m_graph.add_vertex();
            m_vcmap[vc_lhs] = ev_lhs;
            m_rev_vcmap[ev_lhs] = vc_lhs;
            p.lhs = ev_lhs;
            assign_vt_dependencies(vc_lhs, wp.lhs_id, ev_lhs);
        } else {
            p.lhs = iter->second;
        }
        iter = m_vcmap.find(vc_rhs);
        if (iter == m_vcmap.end()) {
            uint64_t ev_rhs = m_graph.add_vertex();
            m_vcmap[vc_rhs] = ev_rhs;
            m_rev_vcmap[ev_rhs] = vc_rhs;
            p.rhs = ev_rhs;
            assign_vt_dependencies(vc_rhs, wp.rhs_id, ev_rhs);
        } else {
            p.rhs = iter->second;
        }

        assert(m_graph.exists(p.lhs) && m_graph.exists(p.rhs));

        int resolve = m_graph.compute_order(p.lhs, p.rhs);

        if (resolve < 0) {
            m_repl_resp[num_pairs] = chronos_cmp_to_byte(CHRONOS_HAPPENS_BEFORE);

            if (p.order != CHRONOS_HAPPENS_BEFORE) {
                if (!(p.flags & CHRONOS_SOFT_FAIL)) {
                    m_repl_resp[num_pairs] = chronos_cmp_to_byte(CHRONOS_WOULDLOOP);
                    break;
                }
            }

        } else if (resolve > 0) {
            m_repl_resp[num_pairs] = chronos_cmp_to_byte(CHRONOS_HAPPENS_AFTER);

            if (p.order != CHRONOS_HAPPENS_AFTER) {
                if (!(p.flags & CHRONOS_SOFT_FAIL)) {
                    m_repl_resp[num_pairs] = chronos_cmp_to_byte(CHRONOS_WOULDLOOP);
                    break;
                }
            }

        } else {
            switch (p.order) {

                case CHRONOS_HAPPENS_BEFORE:
                    m_repl_resp[num_pairs] = chronos_cmp_to_byte(CHRONOS_HAPPENS_BEFORE);
                    m_graph.add_edge(p.lhs, p.rhs);
                    break;

                case CHRONOS_HAPPENS_AFTER:
                    m_repl_resp[num_pairs] = chronos_cmp_to_byte(CHRONOS_HAPPENS_AFTER);
                    m_graph.add_edge(p.rhs, p.lhs);
                    break;

                default:
                    rsm_log(ctx, "should not reach here");
                    abort();
                    break;
            }
        }
    }

    if (num_pairs != NUM_PAIRS) {
        for (size_t i = 0; i < edges.size(); ++i) {
            m_graph.remove_edge(edges[i].first, edges[i].second);
        }
    }

    char *resp_ptr = &m_repl_resp.front();
    rsm_set_output(ctx, resp_ptr, resp_sz);
}

void
chronosd :: gc_weaver_event(uint64_t event_id)
{
    m_graph.decref(event_id);
    auto revmap_iter = m_rev_vcmap.find(event_id);
    assert(revmap_iter != m_rev_vcmap.end());
    m_vcmap.erase(revmap_iter->second);
    m_rev_vcmap.erase(revmap_iter);
}

void
chronosd :: weaver_cleanup(struct rsm_context* ctx,
                           const char* data, size_t data_sz)
{
    ++m_count_weaver_cleanup;
    assert(data_sz == (sizeof(uint64_t)*ClkSz + sizeof(uint64_t)));
    std::vector<uint64_t> cur_cleanup_clk;
    uint64_t vt_id;
    data = unpack_vector_uint64(data, cur_cleanup_clk, ClkSz);
    e::unpack64le(data, &vt_id);

    m_cleanup_clk[vt_id] = cur_cleanup_clk;

    std::vector<uint64_t> cleanup_clk;
    cleanup_clk.reserve(ClkSz);
    for (uint64_t i = 0; i < ClkSz; i++) {
        uint64_t this_clk = UINT64_MAX;
        for (uint64_t j = 0; j < NumVts; j++) {
            if (m_cleanup_clk[j][i] < this_clk) {
                this_clk = m_cleanup_clk[j][i];
            }
        }
        cleanup_clk.emplace_back(this_clk);
    }

    uint64_t decref_count = 0;
    uint64_t epoch = cleanup_clk[0];

    if (epoch < m_vtlist.size()) {
        for (uint64_t i = 0; i < epoch; i++) {
            for (const pair_set_t &set: m_vtlist[i]) {
                for (const auto &p: set) {
                    gc_weaver_event(p.second);
                    decref_count++;
                }
            }
            m_vtlist[i].clear();
        }

        assert(m_vtlist[epoch].size() == NumVts);
        // ClkSz = NumVts+1
        for (uint64_t j = 1; j < ClkSz; j++) {
            pair_set_t &set = m_vtlist[epoch][j-1];
            auto start_iter = set.begin();
            auto end_iter = set.begin();
            bool clean;
            for (; end_iter != set.end(); end_iter++) {
                const std::vector<uint64_t> &this_clk = m_rev_vcmap[end_iter->second];
                clean = true;
                for (uint64_t k = 1; k < this_clk.size(); k++) { // skip comparing epoch
                    if (this_clk[k] >= cleanup_clk[k]) {
                        clean = false;
                        break;
                    }
                }
                if (clean) {
                    gc_weaver_event(end_iter->second);
                    decref_count++;
                } else {
                    break;
                }
            }

            if (end_iter == set.end()) {
                set.clear();
            } else if (start_iter != end_iter) {
                set.erase(start_iter, end_iter);
            }
        }
    }

    const size_t resp_sz = sizeof(uint64_t);
    m_repl_resp.resize(resp_sz);
    char *resp_ptr = &m_repl_resp.front();
    e::pack64le(decref_count, resp_ptr);
    rsm_set_output(ctx, resp_ptr, resp_sz);
}

void
chronosd :: get_stats(struct rsm_context* ctx,
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
    st.count_weaver_cleanup = m_count_weaver_cleanup;

    const size_t resp_sz = sizeof(uint64_t) * 11 + sizeof(uint32_t);
    m_repl_resp.resize(resp_sz);
    char *resp_ptr = &m_repl_resp.front();
    resp_ptr = e::pack64le(st.time, resp_ptr);
    resp_ptr = e::pack64le(st.utime, resp_ptr);
    resp_ptr = e::pack64le(st.stime, resp_ptr);
    resp_ptr = e::pack32le(st.maxrss, resp_ptr);
    resp_ptr = e::pack64le(st.events, resp_ptr);
    resp_ptr = e::pack64le(st.count_create_event, resp_ptr);
    resp_ptr = e::pack64le(st.count_acquire_references, resp_ptr);
    resp_ptr = e::pack64le(st.count_release_references, resp_ptr);
    resp_ptr = e::pack64le(st.count_query_order, resp_ptr);
    resp_ptr = e::pack64le(st.count_assign_order, resp_ptr);
    resp_ptr = e::pack64le(st.count_weaver_order, resp_ptr);
    resp_ptr = e::pack64le(st.count_weaver_cleanup, resp_ptr);
    resp_ptr = &m_repl_resp.front();

    rsm_set_output(ctx, resp_ptr, resp_sz);
}

// not in use right now
void
chronosd :: new_epoch(struct rsm_context* /*ctx*/,
                      const char*, size_t)
{
    //assert(m_vtlist.size() == NumVts);

    //for (pair_set_t &p: m_vtlist) {
    //    p.clear();
    //}

    //const size_t resp_sz = sizeof(uint32_t);
    //m_repl_resp.resize(resp_sz);
    //char *resp_ptr = &m_repl_resp.front();
    //uint32_t success = 0;
    //e::pack32le(success, resp_ptr);
    //rsm_set_output(ctx, resp_ptr, resp_sz);
}

chronosd*
chronosd :: recreate(rsm_context *ctx,
                     const char *data, size_t data_sz)
{
    std::unique_ptr<chronosd> c(new chronosd());

    if (!c)
    {
        rsm_log(ctx, "memory allocation failed\n");
        return nullptr;
    }

    e::unpacker up(data, data_sz);
    up = up >> c->m_graph
            >> c->m_count_create_event
            >> c->m_count_acquire_references
            >> c->m_count_release_references
            >> c->m_count_query_order
            >> c->m_count_assign_order
            >> c->m_count_weaver_order
            >> c->m_count_weaver_cleanup;
    message::unpack_buffer(up, c->m_vcmap);
    message::unpack_buffer(up, c->m_rev_vcmap);
    message::unpack_buffer(up, c->m_vtlist);
    message::unpack_buffer(up, c->m_cleanup_clk);

    if (up.error())
    {
        rsm_log(ctx, "unpacking failed\n");
        return nullptr;
    }

    return c.release();
}

int
chronosd ::snapshot(rsm_context* /* CANNOT USE ctx IN snaphot */,
                    char **data, size_t *data_sz)
{
    size_t sz = pack_size(m_graph)
              + e::pack_size(m_count_create_event)
              + e::pack_size(m_count_acquire_references)
              + e::pack_size(m_count_release_references)
              + e::pack_size(m_count_query_order)
              + e::pack_size(m_count_assign_order)
              + e::pack_size(m_count_weaver_order)
              + e::pack_size(m_count_weaver_cleanup)
              + message::size(m_vcmap)
              + message::size(m_rev_vcmap)
              + message::size(m_vtlist)
              + message::size(m_cleanup_clk);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
    std::auto_ptr<e::buffer> buf(e::buffer::create(sz));
#pragma GCC diagnostic pop
    e::packer pa = buf->pack_at(0);
    pa = pa << m_graph
            << m_count_create_event
            << m_count_acquire_references
            << m_count_release_references
            << m_count_query_order
            << m_count_assign_order
            << m_count_weaver_order
            << m_count_weaver_cleanup;
    message::pack_buffer(pa, m_vcmap);
    message::pack_buffer(pa, m_rev_vcmap);
    message::pack_buffer(pa, m_vtlist);
    message::pack_buffer(pa, m_cleanup_clk);

    char* ptr = static_cast<char*>(malloc(buf->size()));
    *data = ptr;
    *data_sz = buf->size();

    if (*data)
    {
        memmove(ptr, buf->data(), buf->size());
    }

    return 0;
}

extern "C"
{

void*
chronosd_create(struct rsm_context*)
{
    return new (std::nothrow) chronosd();
}

void*
chronosd_recreate(struct rsm_context* ctx,
                  const char *data, size_t data_sz)
{
    return chronosd::recreate(ctx, data, data_sz);
}

int
chronosd_snapshot(struct rsm_context* ctx,
                  void* obj, char** data, size_t* data_sz)
{
    return static_cast<chronosd*>(obj)->snapshot(ctx, data, data_sz);
}

void
chronosd_create_event(struct rsm_context* ctx, void* obj,
                          const char* data, size_t data_sz)
{
    static_cast<chronosd*>(obj)->create_event(ctx, data, data_sz);
}

void
chronosd_acquire_references(struct rsm_context* ctx, void* obj,
                                const char* data, size_t data_sz)
{
    static_cast<chronosd*>(obj)->acquire_references(ctx, data, data_sz);
}

void
chronosd_release_references(struct rsm_context* ctx, void* obj,
                                const char* data, size_t data_sz)
{
    static_cast<chronosd*>(obj)->release_references(ctx, data, data_sz);
}

void
chronosd_query_order(struct rsm_context* ctx, void* obj,
                         const char* data, size_t data_sz)
{
    static_cast<chronosd*>(obj)->query_order(ctx, data, data_sz);
}

void
chronosd_assign_order(struct rsm_context* ctx, void* obj,
                          const char* data, size_t data_sz)
{
    static_cast<chronosd*>(obj)->assign_order(ctx, data, data_sz);
}

void
chronosd_weaver_order(struct rsm_context* ctx, void* obj,
                          const char* data, size_t data_sz)
{
    static_cast<chronosd*>(obj)->weaver_order(ctx, data, data_sz);
}

void
chronosd_weaver_cleanup(struct rsm_context* ctx, void* obj,
                          const char* data, size_t data_sz)
{
    static_cast<chronosd*>(obj)->weaver_cleanup(ctx, data, data_sz);
}

void
chronosd_get_stats(struct rsm_context* ctx, void* obj,
                       const char* data, size_t data_sz)
{
    static_cast<chronosd*>(obj)->get_stats(ctx, data, data_sz);
}

void
chronosd_new_epoch(struct rsm_context* ctx, void* obj,
                       const char* data, size_t data_sz)
{
    static_cast<chronosd*>(obj)->new_epoch(ctx, data, data_sz);
}

} // extern "C"
