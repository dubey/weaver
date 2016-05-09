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

#define weaver_debug_
// Chronos
#include "common/weaver_constants.h"
#include "common/stl_serialization.h"
#include "event_dependency_graph.h"

#define EDGES_EMPTY (UINT64_MAX - 1)
#define EDGES_END (UINT64_MAX - 1)
#define CACHE_SIZE 16384ULL
#define CACHE_MALLOC_SIZE (CACHE_SIZE * 64ULL)

event_dependency_graph :: event_dependency_graph()
    : m_nextid(1)
    , m_vertices_number(0)
    , m_vertices_allocated(0)
    , m_free_inner_id_end(0)
    , m_cache()
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

    m_cache = static_cast<uint64_t*>(mmap(NULL, CACHE_MALLOC_SIZE, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE, -1, 0));

    assert(m_cache != MAP_FAILED);
    memset(m_cache, 0, CACHE_MALLOC_SIZE);
}

event_dependency_graph :: ~event_dependency_graph() throw ()
{
    if (m_base)
    {
        munmap(m_free_inner_ids, base_size(m_vertices_allocated));
    }
}

uint64_t
event_dependency_graph :: add_vertex()
{
    uint64_t event_id = m_nextid;
    uint64_t inner_id = 0;

    if (m_free_inner_id_end == 0)
    {
        if (m_vertices_allocated == 0 ||
            m_vertices_number >= m_vertices_allocated)
        {
            resize();
        }

        assert(m_vertices_number < m_vertices_allocated);
        inner_id = m_vertices_number;
        ++m_vertices_number;
        m_edges[inner_id] = NULL;
    }
    else
    {
        inner_id = m_free_inner_ids[m_free_inner_id_end - 1];
        --m_free_inner_id_end;
        // don't touch m_edges;
    }

    m_inner_to_event[inner_id] = event_id;
    ++m_nextid;
    m_refcount[inner_id] = 1;
    m_event_to_inner.insert(std::make_pair(event_id, inner_id));

    if (m_edges[inner_id])
    {
        *m_edges[inner_id] = EDGES_EMPTY;
    }

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

    poke_cache(src_event_id, dst_event_id);
    uint64_t* edges = m_edges[inner_src];

    if (!edges)
    {
        m_edges[inner_src] = edges = new uint64_t[2];
        edges[0] = EDGES_EMPTY;
        edges[1] = EDGES_END;
    }

    while (*edges != EDGES_EMPTY && *edges != EDGES_END)
    {
        ++edges;
    }

    if (*edges == EDGES_END)
    {
        size_t sz = (edges - m_edges[inner_src]) + 1;
        uint64_t* new_edges = new uint64_t[sz * 2];
        memmove(new_edges, m_edges[inner_src], sz * sizeof(uint64_t));
        new_edges[2 * sz - 1] = EDGES_END;
        delete[] m_edges[inner_src];
        m_edges[inner_src] = new_edges;
        edges = new_edges + sz - 1;
    }

    *edges = inner_dst;
    ++edges;

    if (*edges != EDGES_END)
    {
        *edges = EDGES_EMPTY;
    }

    assert(m_refcount[inner_dst] < UINT64_MAX);
    ++m_refcount[inner_dst];
}

void
event_dependency_graph :: remove_edge(uint64_t src_event_id, uint64_t dst_event_id)
{
    uint64_t inner_src;
    uint64_t inner_dst;
    bool found;

    found = map(src_event_id, &inner_src);
    assert(found);
    found = map(dst_event_id, &inner_dst);
    assert(found);

    uint64_t* edges = m_edges[inner_src];

    while (*edges != EDGES_EMPTY && *edges != EDGES_END)
    {
        if (*edges == inner_dst)
        {
            break;
        }

        ++edges;
    }

    uint64_t* edges_next = edges + 1;

    while (*edges != EDGES_EMPTY && *edges != EDGES_END)
    {
        if (*edges_next == EDGES_END)
        {
            *edges = EDGES_EMPTY;
        }
        else
        {
            *edges = *edges_next;
        }

        ++edges;
        ++edges_next;
    }

    assert(m_refcount[inner_dst] > 1);
    --m_refcount[inner_dst];
}

int
event_dependency_graph :: compute_order(uint64_t lhs_event_id, uint64_t rhs_event_id)
{
    if (check_cache(lhs_event_id, rhs_event_id))
    {
        return -1;
    }

    if (check_cache(rhs_event_id, lhs_event_id))
    {
        return 1;
    }

    uint64_t inner_lhs;
    uint64_t inner_rhs;
    bool found;

    found = map(lhs_event_id, &inner_lhs);
    assert(found);
    found = map(rhs_event_id, &inner_rhs);
    assert(found);
    uint64_t dense_sz = 0;

    if (bfs(inner_lhs, inner_rhs, &dense_sz))
    {
        poke_cache(lhs_event_id, rhs_event_id);
        return -1;
    }

    if (bfs(inner_rhs, inner_lhs, &dense_sz))
    {
        poke_cache(rhs_event_id, lhs_event_id);
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
    uint64_t inner;
    bool found = map(event_id, &inner);

    if (!found)
    {
        return false;
    }

    inner_decref(inner);
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
event_dependency_graph :: bfs(uint64_t start, uint64_t end, uint64_t* dense_sz)
{
    assert(m_bfsqueue);
    uint64_t bfshead = 0;
    uint64_t bfstail = 1;
    m_bfsqueue[bfshead] = start;
    m_bfsqueue[bfstail] = UINT64_MAX;

    while (m_bfsqueue[bfshead] != UINT64_MAX)
    {
        uint64_t* edges = m_edges[m_bfsqueue[bfshead]];

        while (edges && *edges != EDGES_EMPTY && *edges != EDGES_END)
        {
            uint64_t e = *edges;
            ++edges;

            // Check if this is the final edge in the path
            if (e == end)
            {
                return true;
            }

            // If it's in the set
            if (m_sparse[e] < *dense_sz && m_dense[m_sparse[e]] == e)
            {
                continue;
            }

            // Add it to the set
            m_sparse[e] = *dense_sz;
            m_dense[*dense_sz] = e;
            ++*dense_sz;

            m_bfsqueue[bfstail] = e;
            ++bfstail;
            m_bfsqueue[bfstail] = UINT64_MAX;
        }

        ++bfshead;
    }

    return false;
}

void
event_dependency_graph :: inner_decref(uint64_t inner)
{
    assert(m_bfsqueue);
    uint64_t dense_sz = 0;
    uint64_t bfshead = 0;
    uint64_t bfstail = 1;
    m_bfsqueue[bfshead] = inner;
    m_bfsqueue[bfstail] = UINT64_MAX;

    while (m_bfsqueue[bfshead] != UINT64_MAX)
    {
        uint64_t v = m_bfsqueue[bfshead];
        ++bfshead;
        assert(m_refcount[v] > 0);
        --m_refcount[v];

        if (m_refcount[v] > 0)
        {
            continue;
        }

        uint64_t* edges = m_edges[v];

        while (edges && *edges != EDGES_EMPTY && *edges != EDGES_END)
        {
            uint64_t e = *edges;
            ++edges;

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

        m_event_to_inner.erase(m_inner_to_event[v]);
        m_free_inner_ids[m_free_inner_id_end] = v;
        ++m_free_inner_id_end;
    }
}

uint64_t
event_dependency_graph :: base_size(uint64_t step_size) const
{
    return (7 * step_size * sizeof(uint64_t) + sizeof(uint64_t));
}

void
event_dependency_graph :: allocate_base(void **new_base, uint64_t next_step) const
{
    uint64_t allocate = base_size(next_step);
    *new_base = mmap(NULL, allocate, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE, -1, 0);

    if (*new_base == MAP_FAILED)
    {
        throw std::bad_alloc();
    }
}

void
event_dependency_graph :: reassign_base(void *new_base, uint64_t next_step, bool copy)
{
    uint64_t* tmp = reinterpret_cast<uint64_t*>(new_base);

    if (copy) {
        // Copy the free inner ids
        memmove(tmp, m_free_inner_ids, m_free_inner_id_end * sizeof(uint64_t));
    }
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
    tmp += next_step + 1;

    if (copy) {
        // Copy inner->event mapping
        memmove(tmp, m_inner_to_event, m_vertices_allocated * sizeof(uint64_t));
    }
    m_inner_to_event = tmp;
    tmp += next_step;

    if (copy) {
        // Copy refcounts
        memmove(tmp, m_refcount, m_vertices_allocated * sizeof(uint64_t));
    }
    m_refcount = tmp;
    tmp += next_step;

    if (copy) {
        // Copy edge pointers
        memmove(tmp, m_edges, m_vertices_allocated * sizeof(uint64_t));
    }
    m_edges = reinterpret_cast<uint64_t**>(tmp);

    // Give back what ye taketh
    if (m_base) {
        munmap(m_base, base_size(m_vertices_allocated));
    }

    // Save the current memory
    m_base = new_base;

    // Update allocation size;
    m_vertices_allocated = next_step;
}

void
event_dependency_graph :: resize()
{
    uint64_t next_step = 0;

    if (m_vertices_allocated == 0)
    {
        next_step = 4;
    }
    else
    {
        next_step = m_vertices_allocated;
    }

    next_step *= 1.3;
    void *new_base;
    allocate_base(&new_base, next_step);

    reassign_base(new_base, next_step, true);
}

#define CACHE_LINE(S, D) ((((S) & 4095) << 12) | ((D) & 4095)) 

bool
event_dependency_graph :: check_cache(uint64_t src, uint64_t dst)
{
    uint64_t cache_line = CACHE_LINE(src, dst);
    cache_line &= (CACHE_SIZE - 1);

    for (size_t i = 0; i < 4; ++i)
    {
        if (m_cache[cache_line + 2 * i] == src &&
            m_cache[cache_line + 2 * i + 1] == dst)
        {
            return true;
        }
    }

    return false;
}

void
event_dependency_graph :: poke_cache(uint64_t src, uint64_t dst)
{
    uint64_t cache_line = CACHE_LINE(src, dst);
    cache_line &= (CACHE_SIZE - 1);
    m_cache[cache_line + 7] = m_cache[cache_line + 5];
    m_cache[cache_line + 6] = m_cache[cache_line + 4];
    m_cache[cache_line + 5] = m_cache[cache_line + 3];
    m_cache[cache_line + 4] = m_cache[cache_line + 2];
    m_cache[cache_line + 3] = m_cache[cache_line + 1];
    m_cache[cache_line + 2] = m_cache[cache_line + 0];
    m_cache[cache_line + 1] = dst;
    m_cache[cache_line + 0] = src;
}

e::packer
operator << (e::packer lhs, const event_dependency_graph &rhs)
{
    e::slice cache_slice((const uint8_t*)rhs.m_cache, CACHE_MALLOC_SIZE);
    e::slice base_slice;
    if (rhs.m_base) {
        base_slice = e::slice((const uint8_t*)rhs.m_base, rhs.base_size(rhs.m_vertices_allocated));
    } else {
        base_slice = e::slice((const uint8_t*)rhs.m_base, (size_t)0);
    }

    lhs = lhs << rhs.m_nextid
              << rhs.m_vertices_number
              << rhs.m_vertices_allocated
              << rhs.m_free_inner_id_end
              << cache_slice
              << base_slice;
    message::pack_buffer(lhs, nullptr, rhs.m_event_to_inner);

    return lhs;
}

e::unpacker
operator >> (e::unpacker lhs, event_dependency_graph &rhs)
{
    e::slice cache_slice, base_slice;

    lhs = lhs >> rhs.m_nextid
              >> rhs.m_vertices_number
              >> rhs.m_vertices_allocated
              >> rhs.m_free_inner_id_end
              >> cache_slice
              >> base_slice;
    message::unpack_buffer(lhs, nullptr, rhs.m_event_to_inner);

    assert(rhs.base_size(rhs.m_vertices_allocated) == base_slice.size());

    void *new_base = NULL;
    rhs.m_base = NULL;
    if (base_slice.size() > 0) {
        rhs.allocate_base(&new_base, rhs.m_vertices_allocated);
        memmove(new_base, base_slice.data(), rhs.base_size(rhs.m_vertices_allocated));

        rhs.reassign_base(new_base, rhs.m_vertices_allocated, false);
    }

    return lhs;
}

size_t
pack_size(const event_dependency_graph &g)
{
    e::slice cache_slice((const uint8_t*)g.m_cache, CACHE_MALLOC_SIZE);
    e::slice base_slice((const uint8_t*)g.m_base, g.base_size(g.m_vertices_allocated));

    return e::pack_size(g.m_nextid)
         + e::pack_size(g.m_vertices_number)
         + e::pack_size(g.m_vertices_allocated)
         + e::pack_size(g.m_free_inner_id_end)
         + e::pack_size(cache_slice)
         + e::pack_size(base_slice)
         + message::size(nullptr, g.m_event_to_inner);
}
