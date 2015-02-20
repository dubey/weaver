/*
 * ===============================================================
 *    Description:  Implement get node program. 
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2014, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "common/message.h"
#include "node_prog/node_get_program.h"

using node_prog::search_type;
using node_prog::node_get_params;
using node_prog::node_get_state;
using node_prog::cache_response;

uint64_t
node_get_params :: size() const 
{
    return message::size(node);
}

void
node_get_params :: pack(e::buffer::packer& packer) const
{
    message::pack_buffer(packer, node);
}

void
node_get_params :: unpack(e::unpacker& unpacker)
{
    message::unpack_buffer(unpacker, node);
}

std::pair<search_type, std::vector<std::pair<db::remote_node, node_get_params>>>
node_prog :: node_get_node_program(
        node &n,
        db::remote_node &,
        node_get_params &params,
        std::function<node_get_state&()>,
        std::function<void(std::shared_ptr<node_prog::Cache_Value_Base>,
            std::shared_ptr<std::vector<db::remote_node>>, cache_key_t)>&,
        cache_response<Cache_Value_Base>*)
{
    n.get_client_node(params.node);

    return std::make_pair(search_type::DEPTH_FIRST, std::vector<std::pair<db::remote_node, node_get_params>>
            (1, std::make_pair(db::coordinator, std::move(params)))); 
}
