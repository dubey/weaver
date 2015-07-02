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

#define weaver_debug_
#include "common/stl_serialization.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/edge_get_program.h"

using node_prog::search_type;
using node_prog::edge_get_params;
using node_prog::edge_get_state;
using node_prog::cache_response;

uint64_t edge_get_params :: size() const 
{
    uint64_t toRet = message::size(nbrs)
        + message::size(request_edges)
        + message::size(response_edges)
        + message::size(properties);
    return toRet;
}

void edge_get_params :: pack(e::packer& packer) const 
{
    message::pack_buffer(packer, nbrs);
    message::pack_buffer(packer, request_edges);
    message::pack_buffer(packer, response_edges);
    message::pack_buffer(packer, properties);
}

void edge_get_params :: unpack(e::unpacker& unpacker)
{
    message::unpack_buffer(unpacker, nbrs);
    message::unpack_buffer(unpacker, request_edges);
    message::unpack_buffer(unpacker, response_edges);
    message::unpack_buffer(unpacker, properties);
}

bool
check_edge_nbrs(const std::vector<node_handle_t> &nbrs,
                node_prog::edge &e)
{
    bool return_edge = true;

    if (!nbrs.empty()) {
        return_edge = false;
        for (const node_handle_t &nbr: nbrs) {
            if (nbr == e.get_neighbor().handle) {
                return_edge = true;
                break;
            }
        }
    }

    return return_edge;
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
    if (!params.request_edges.empty()) {
        // don't have to iterate over all edges
        for (const edge_handle_t &h: params.request_edges) {
            if (n.edge_exists(h)) {
                edge &e = n.get_edge(h);
                bool return_edge = true;

                if (!params.nbrs.empty()) {
                    return_edge = check_edge_nbrs(params.nbrs, e);
                }

                if (return_edge && !e.has_all_properties(params.properties)) {
                    return_edge = false;
                }

                if (return_edge) {
                    cl::edge cl_edge;
                    e.get_client_edge(n.get_handle(), cl_edge);
                    params.response_edges.emplace_back(cl_edge);
                }
            }
        }
    } else {
        // iterate over all edges, will be slow for vertices with lots of edges
        for (edge &e: n.get_edges()) {
            bool return_edge = true;

            if (!params.nbrs.empty()) {
                return_edge = check_edge_nbrs(params.nbrs, e);
            }

            if (return_edge && !params.request_edges.empty()) {
                return_edge = false;
                for (const edge_handle_t &h: params.request_edges) {
                    WDEBUG << "request_edge=" << h
                           << ", handle=" << e.get_handle() << std::endl;
                    if (e.get_handle() == h) {
                        return_edge = true;
                        break;
                    }
                }
            }

            if (return_edge && e.has_all_properties(params.properties)) {
                cl::edge cl_edge;
                e.get_client_edge(n.get_handle(), cl_edge);
                params.response_edges.emplace_back(cl_edge);
            }
        }
    }

    return std::make_pair(search_type::DEPTH_FIRST, std::vector<std::pair<db::remote_node, edge_get_params>>
            (1, std::make_pair(db::coordinator, std::move(params)))); 
}
