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

#define __STDC_LIMIT_MACROS

// C
#include <cassert>

// STL
#include <algorithm>
#include <queue>

// eos
#include "event_dependency_graph.h"

class event_dependency_graph::vertex
{
    public:
        vertex();
        ~vertex() throw () {}

    public:
        void clear();

        void add_edge(uint64_t inner);
        const std::vector<uint64_t>& edges() { return m_edges; }

        void incref() { assert(m_refcount < UINT64_MAX); ++m_refcount; }
        void decref() { assert(m_refcount > 0); --m_refcount; }
        uint64_t refcount() { return m_refcount; }

    public:
        uint64_t event;
        uint64_t sparse;
        uint64_t bfsnext;

    public:
        uint64_t m_refcount;
        // The other half to avoid uninitialized memory
        std::vector<uint64_t> m_edges;
};

event_dependency_graph :: event_dependency_graph()
    : m_nextid(1)
    , m_vertices()
    , m_event_to_inner()
    , m_free_inner_ids()
    , m_dense()
{
}

event_dependency_graph :: ~event_dependency_graph() throw ()
{
}

uint64_t
event_dependency_graph :: add_vertex()
{
    uint64_t event_id = m_nextid;
    uint64_t inner_id = 0;

    if (m_free_inner_ids.empty())
    {
        m_vertices.push_back(vertex());

        if (m_dense.size() < m_vertices.capacity())
        {
            m_dense.resize(m_vertices.capacity());
        }

        inner_id = m_vertices.size() - 1;
    }
    else
    {
        m_vertices[m_free_inner_ids.back()].clear();
        inner_id = m_free_inner_ids.back();
        m_free_inner_ids.pop_back();
    }

    m_vertices[inner_id].event = event_id;
    m_vertices[inner_id].incref();
    m_event_to_inner.insert(std::make_pair(event_id, inner_id));
    ++m_nextid;
    return event_id;
}

void
event_dependency_graph :: add_edge(uint64_t src_event_id, uint64_t dst_event_id)
{
    uint64_t inner_src;
    uint64_t inner_dst;
    bool found;

    found = map(src_event_id, &inner_src);
    assert(found);
    found = map(dst_event_id, &inner_dst);
    assert(found);
    m_vertices[inner_src].add_edge(inner_dst);
    m_vertices[inner_dst].incref();
}

int
event_dependency_graph :: compute_order(uint64_t lhs_event_id, uint64_t rhs_event_id)
{
    uint64_t inner_lhs;
    uint64_t inner_rhs;
    bool found;

    found = map(lhs_event_id, &inner_lhs);
    assert(found);
    found = map(rhs_event_id, &inner_rhs);
    assert(found);

    if (bfs(inner_lhs, inner_rhs))
    {
        return -1;
    }

    if (bfs(inner_rhs, inner_lhs))
    {
        return 1;
    }

    return 0;
}

bool
event_dependency_graph :: exists(uint64_t event_id)
{
    uint64_t inner;
    return map(event_id, &inner);
}

bool
event_dependency_graph :: incref(uint64_t event_id)
{
    uint64_t inner;
    bool found = map(event_id, &inner);
    
    if (found)
    {
        m_vertices[inner].incref();
    }

    return found;
}

bool
event_dependency_graph :: decref(uint64_t event_id)
{
    uint64_t v;
    bool found = map(event_id, &v);

    if (!found)
    {
        return false;
    }

    std::queue<uint64_t> toremove;
    toremove.push(v);

    while (!toremove.empty())
    {
        v = toremove.front();
        toremove.pop();
        m_vertices[v].decref();

        if (m_vertices[v].refcount() > 0)
        {
            continue;
        }

        for (size_t i = 0; i < m_vertices[v].edges().size(); ++i)
        {
            toremove.push(m_vertices[v].edges()[i]);
        }

        m_event_to_inner.erase(m_vertices[v].event);
        m_free_inner_ids.push_back(v);
    }

    return true;
}

bool
event_dependency_graph :: map(uint64_t event, uint64_t* inner)
{
    event_map_t::iterator iter = m_event_to_inner.find(event);

    if (iter == m_event_to_inner.end())
    {
        return false;
    }
    else
    {
        *inner = iter->second;
        return true;
    }
}

bool
event_dependency_graph :: bfs(uint64_t start, uint64_t end)
{
    m_dense.clear();
    uint64_t bfshead = start;
    uint64_t bfstail = start;
    m_vertices[start].bfsnext = UINT64_MAX;

    while (bfshead != UINT64_MAX)
    {
        std::vector<uint64_t>::const_iterator edge = m_vertices[bfshead].edges().begin();
        std::vector<uint64_t>::const_iterator edge_end = m_vertices[bfshead].edges().end();

        for (; edge != edge_end; ++edge)
        {
            // If it's in the set
            if (m_vertices[*edge].sparse < m_dense.size() &&
                m_dense[m_vertices[*edge].sparse] == *edge)
            {
                continue;
            }

            // Check if this is the final edge in the path
            if (*edge == end)
            {
                return true;
            }

            // Add it to the set and pending queue
            m_vertices[*edge].sparse = m_dense.size();
            m_dense.push_back(*edge);

            // Add it to the end of the bfs queue
            m_vertices[bfstail].bfsnext = *edge;
            bfstail = *edge;
            m_vertices[*edge].bfsnext = UINT64_MAX;
        }

        // Advance
        bfshead = m_vertices[bfshead].bfsnext;
    }

    return false;
}

event_dependency_graph :: vertex :: vertex()
    : event(0)
    , sparse(0)
    , bfsnext(0)
    , m_refcount(0)
    , m_edges()
{
}

void
event_dependency_graph :: vertex :: clear()
{
    event = 0;
    sparse = 0;
    bfsnext = 0;
    m_refcount = 0;
    m_edges.clear();
}

void
event_dependency_graph :: vertex :: add_edge(uint64_t inner)
{
    if (std::find(m_edges.begin(), m_edges.end(), inner) == m_edges.end())
    {
        m_edges.push_back(inner);
    }
}
