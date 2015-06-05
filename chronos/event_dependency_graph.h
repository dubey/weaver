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

#ifndef event_dependency_graph_h_
#define event_dependency_graph_h_

// C
#include <stdint.h>

// STL
#include <tr1/unordered_map>
#include <vector>

// Google SparseHash
#include <google/sparse_hash_map>

// e
#include <e/serialization.h>

// Weaver
#include "common/utils.h"

class event_dependency_graph
{
    public:
        event_dependency_graph();
        ~event_dependency_graph() throw ();

    public:
        uint64_t add_vertex();
        void add_edge(uint64_t src_event_id, uint64_t dst_event_id);
        void remove_edge(uint64_t src_event_id, uint64_t dst_event_id);
        int compute_order(uint64_t lhs_event_id, uint64_t rhs_event_id);
        bool exists(uint64_t event_id);
        uint64_t num_vertices();
        bool incref(uint64_t event_id);
        bool decref(uint64_t event_id);

    public:
        typedef google::sparse_hash_map<uint64_t, uint64_t,
                std::tr1::hash<uint64_t>, weaver_util::equint64_t> event_map_t;

    private:
        event_dependency_graph(const event_dependency_graph&);

    private:
        bool map(uint64_t event, uint64_t* inner);
        bool bfs(uint64_t start, uint64_t end, uint64_t* dense_sz);
        void inner_decref(uint64_t inner);
        uint64_t base_size(uint64_t step_size) const;
        void allocate_base(void **new_base, uint64_t step_size) const;
        void reassign_base(void *new_base, uint64_t step_size, bool copy);
        void resize();
        // Cache that shit
        bool check_cache(uint64_t outer_src, uint64_t inner_dst);
        void poke_cache(uint64_t outer_src, uint64_t inner_dst);

    private:
        event_dependency_graph& operator = (const event_dependency_graph&);

    private:
        friend e::packer operator << (e::packer lhs, const event_dependency_graph &rhs);
        friend e::unpacker operator >> (e::unpacker lhs, event_dependency_graph &rhs);
        friend size_t pack_size(const event_dependency_graph &g);

    private:
        uint64_t m_nextid;
        uint64_t m_vertices_number;
        uint64_t m_vertices_allocated;
        uint64_t m_free_inner_id_end;
        uint64_t* m_cache;
        void* m_base;
        uint64_t* m_free_inner_ids;
        uint64_t* m_dense;
        uint64_t* m_sparse;
        uint64_t* m_bfsqueue;
        uint64_t* m_inner_to_event;
        uint64_t* m_refcount;
        uint64_t** m_edges;
        event_map_t m_event_to_inner;
};

e::packer
operator << (e::packer lhs, const event_dependency_graph &rhs);

e::unpacker
operator >> (e::unpacker lhs, event_dependency_graph &rhs);

size_t
pack_size(const event_dependency_graph &g);

#endif // event_dependency_graph_h_
