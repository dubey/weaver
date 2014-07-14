/*
 * ===============================================================
 *    Description:  Implementation of read-n-edges program.
 *
 *        Created:  2014-05-30 12:21:42
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "common/message.h"
#include "node_prog/edge.h"
#include "node_prog/read_n_edges_program.h"

using node_prog::search_type;
using node_prog::read_n_edges_params;
using node_prog::read_n_edges_state;
using node_prog::cache_response;

uint64_t
read_n_edges_params :: size() const 
{
    uint64_t toRet = message::size(num_edges)
        + message::size(edges_props)
        + message::size(return_edges);
    return toRet;
}

void
read_n_edges_params :: pack(e::buffer::packer& packer) const 
{
    message::pack_buffer(packer, num_edges);
    message::pack_buffer(packer, edges_props);
    message::pack_buffer(packer, return_edges);
}

void
read_n_edges_params :: unpack(e::unpacker& unpacker)
{
    message::unpack_buffer(unpacker, num_edges);
    message::unpack_buffer(unpacker, edges_props);
    message::unpack_buffer(unpacker, return_edges);
}

std::pair<search_type, std::vector<std::pair<db::element::remote_node, read_n_edges_params>>>
node_prog :: read_n_edges_node_program(
    node &n,
    db::element::remote_node &,
    read_n_edges_params &params,
    std::function<read_n_edges_state&()>,
    std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>, std::shared_ptr<std::vector<db::element::remote_node>>, cache_key_t)>&,
    cache_response<Cache_Value_Base>*)
{
    auto elist = n.get_edges();
    int pushcnt = 0;
    for (edge &e : elist) {
        if (e.has_all_properties(params.edges_props)) {
            pushcnt++;
            params.return_edges.emplace_back(e.get_id());
            if (--params.num_edges == 0) {
                break;
            }
        }
    }
    return std::make_pair(search_type::DEPTH_FIRST, std::vector<std::pair<db::element::remote_node, read_n_edges_params>>
            (1, std::make_pair(db::element::coordinator, std::move(params)))); 
}
