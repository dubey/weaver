/*
 * ===============================================================
 *    Description:  Get edge program implementation.
 *
 *        Created:  2014-05-30 12:04:56
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "common/message.h"
#include "node_prog/edge_get_program.h"

using node_prog::search_type;
using node_prog::edge_get_params;
using node_prog::cache_response;

uint64_t edge_get_params :: size() const 
{
    uint64_t toRet = message::size(nbrs)
        + message::size(request_edges)
        + message::size(response_edges);
    return toRet;
}

void edge_get_params :: pack(e::buffer::packer& packer) const 
{
    message::pack_buffer(packer, nbrs);
    message::pack_buffer(packer, request_edges);
    message::pack_buffer(packer, response_edges);
}

void edge_get_params :: unpack(e::unpacker& unpacker)
{
    message::unpack_buffer(unpacker, nbrs);
    message::unpack_buffer(unpacker, request_edges);
    message::unpack_buffer(unpacker, response_edges);
}

std::pair<search_type, std::vector<std::pair<db::remote_node, edge_get_params>>>
node_prog :: edge_get_node_program(
    node &n,
    db::remote_node &,
    edge_get_params &params,
    std::function<edge_get_state&()>,
    std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
        std::shared_ptr<std::vector<db::remote_node>>, cache_key_t)>&,
    cache_response<Cache_Value_Base>*)
{
    auto elist = n.get_edges();
    for (edge &e : elist) {
        bool select = true;

        if (!params.nbrs.empty()) {
            auto nbr = e.get_neighbor();
            bool found = false;
            for (const node_handle_t &h: params.nbrs) {
                if (h == nbr.handle) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                select = false;
            }
        }

        if (select && !params.request_edges.empty()) {
            edge_handle_t this_hndl = e.get_handle();
            bool found = false;
            for (const edge_handle_t &h: params.request_edges) {
                if (this_hndl == h) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                select = false;
            }
        }

        if (select) {
            cl::edge cl_edge;
            e.get_client_edge(cl_edge);
            params.response_edges.emplace_back(cl_edge);
        }
    }

    return std::make_pair(search_type::DEPTH_FIRST, std::vector<std::pair<db::remote_node, edge_get_params>>
            (1, std::make_pair(db::coordinator, std::move(params)))); 
}
