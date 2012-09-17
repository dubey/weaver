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

#define __STDC_LIMIT_MACROS

// C
#include <cassert>

// POSIX
#include <sys/mman.h>

// STL
#include <algorithm>
#include <queue>

// Chronos
#include "event_dependency_graph.h"

event_dependency_graph :: event_dependency_graph()
    : m_nextid(1)
    , m_vertices_number(0)
    , m_vertices_allocated(0)
    , m_free_inner_id_end(0)
    , m_base(NULL)
    , m_free_inner_ids(NULL)
    , m_dense(NULL)
    , m_sparse(NULL)
    , m_bfsqueue(NULL)
    , m_inner_to_event(NULL)
    , m_refcount(NULL)
    , m_edges(NULL)
    , m_event_to_inner()
{
    m_event_to_inner.set_deleted_key(0);
}

event_dependency_graph :: ~event_dependency_graph() throw ()
{
    if (m_base)
    {
        munmap(m_free_inner_ids, m_vertices_allocated * 7 * sizeof(uint64_t));
    }
}

uint64_t
event_dependency_graph :: add_vertex()
{
    uint64_t event_id = m_nextid;
    uint64_t inner_id = 0;

    if (m_free_inner_id_end == 0)
    {
        if (m_vertices_number >= m_vertices_allocated)
        {
            resize();
        }

        assert(m_vertices_number < m_vertices_allocated);
        inner_id = m_vertices_number - 1;
        m_edges[inner_id] = NULL;
    }
    else
    {
        inner_id = m_free_inner_ids[m_free_inner_id_end - 1];
        --m_free_inner_id_end;
        // don't touch m_edges;
    }

    m_inner_to_event[inner_id] = event_id;
    m_refcount[inner_id] = 1;
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

    uint64_t insert = inner_dst;
    uint64_t* edges = m_edges[inner_src];

    if (!edges)
    {
        m_edges[inner_src] = edges = new uint64_t[2];
        edges[0] = 0;
        edges[1] = UINT64_MAX;
    }

    while (*edges != 0 && *edges != UINT64_MAX)
    {
        if (insert < *edges)
        {
            std::swap(insert, *edges);
        }

        ++edges;
    }

    if (*edges == UINT64_MAX)
    {
        size_t sz = edges - m_edges[inner_src];
        uint64_t* new_edges = new uint64_t[(sz + 1) * 2];
        memmove(new_edges, edges, sz * sizeof(uint64_t));
        new_edges[2 * sz + 1] = UINT64_MAX;
        delete[] m_edges[inner_src];
        m_edges[inner_src] = new_edges;
        edges = new_edges + sz;
    }

    *edges = inner_dst;
    ++edges;

    if (*edges != UINT64_MAX)
    {
        *edges = 0;
    }

    assert(m_refcount[inner_dst] < UINT64_MAX);
    ++m_refcount[inner_dst];
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

uint64_t
event_dependency_graph :: num_vertices()
{
    return m_vertices_number - m_free_inner_id_end;
}

bool
event_dependency_graph :: incref(uint64_t event_id)
{
    uint64_t inner;
    bool found = map(event_id, &inner);
    
    if (found)
    {
        assert(m_refcount[inner] < UINT64_MAX);
        ++m_refcount[inner];
    }

    return found;
}

bool
event_dependency_graph :: decref(uint64_t event_id)
{
    return false;
}

#if 0
bool
event_dependency_graph :: decref(uint64_t event_id)
{
    uint64_t toremove;
    bool found = map(event_id, &toremove);

    if (!found)
    {
        return false;
    }

    uint64_t bfshead = toremove;
    uint64_t bfstail = toremove;
    m_vertices[toremove].bfsnext = UINT64_MAX;

    while (bfshead != UINT64_MAX)
    {
        uint64_t v = bfshead;
        bfshead = m_vertices[bfshead].bfsnext;
        m_vertices[bfshead].decref();

        if (m_vertices[v].refcount() > 0)
        {
            continue;
        }

        for (size_t i = 0; i < m_vertices[v].edges().size(); ++i)
        {
            uint64_t u = m_vertices[v].edges()[i];
            m_vertices[u].bfsnext = UINT64_MAX;
            m_vertices[bfstail].bfsnext = u;
            bfstail = u;
        }

        m_event_to_inner.erase(m_vertices[v].event);
        m_free_inner_ids.push_back(v);
    }

    return true;
}
#endif

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
    assert(m_bfsqueue);
    uint64_t dense_sz = 0;
    uint64_t bfshead = 0;
    uint64_t bfstail = 1;
    m_bfsqueue[bfshead] = start;
    m_bfsqueue[bfstail] = UINT64_MAX;

    while (m_bfsqueue[bfshead] != UINT64_MAX)
    {
        uint64_t* edges = m_edges[m_bfsqueue[bfshead]];

        while (*edges != 0 && *edges != UINT64_MAX)
        {
            uint64_t e = *edges;

            // Check if this is the final edge in the path
            if (e == end)
            {
                return true;
            }

            // If it's in the set
            if (m_sparse[e] < dense_sz && m_dense[m_sparse[e]] == e)
            {
                continue;
            }

            // Add it to the set
            m_sparse[e] = dense_sz;
            m_dense[dense_sz] = e;
            ++dense_sz;

            m_bfsqueue[bfstail] = e;
            ++bfstail;
            m_bfsqueue[bfstail] = UINT64_MAX;
        }

        ++bfshead;
    }

    return false;
}

void
event_dependency_graph :: resize()
{
    uint64_t next_step = 0;

    if (m_vertices_allocated == 0)
    {
        next_step = 2;
    }
    else
    {
        next_step = m_vertices_allocated;
    }

    next_step *= 1.3;
    uint64_t allocate = 7 * next_step * sizeof(uint64_t);

    void* new_base = mmap(NULL, allocate, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE, -1, 0);

    if (new_base == MAP_FAILED)
    {
        throw std::bad_alloc();
    }

    uint64_t* tmp = reinterpret_cast<uint64_t*>(new_base);
    // Copy the free inner ids
    memmove(tmp, m_free_inner_ids, m_free_inner_id_end);
    m_free_inner_ids = tmp;
    tmp += next_step;
    // Do nothing for "dense"
    m_dense = tmp;
    tmp += next_step;
    // Do nothing for "sparse"
    m_sparse = tmp;
    tmp += next_step;
    // Do nothing for "bfsqueue"
    m_bfsqueue = tmp;
    tmp += next_step;
    // Copy inner->event mapping
    memmove(tmp, m_inner_to_event, m_vertices_allocated);
    m_inner_to_event = tmp;
    tmp += next_step;
    // Copy refcounts
    memmove(tmp, m_refcount, m_vertices_allocated);
    m_refcount = tmp;
    tmp += next_step;
    // Copy edge pointers
    memmove(tmp, m_edges, m_vertices_allocated);
    m_edges = reinterpret_cast<uint64_t**>(tmp);
    // Give back what ye taketh
    munmap(m_base, 7 * sizeof(uint64_t) * m_vertices_allocated);
    // Save the current memory
    m_base = new_base;
    // Update allocation size;
    m_vertices_allocated = next_step;
}
