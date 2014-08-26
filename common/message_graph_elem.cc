/*
 * ===============================================================
 *    Description:  Node and edge packers, unpackers.
 *
 *        Created:  05/20/2013 02:40:32 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "common/message.h"
#include "common/vclock.h"
#include "db/node.h"
#include "db/edge.h"
#include "db/property.h"
#include "node_prog/property.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/reach_program.h"
#include "node_prog/pathless_reach_program.h"
#include "node_prog/clustering_program.h"
#include "node_prog/read_node_props_program.h"
#include "node_prog/read_edges_props_program.h"
#include "node_prog/read_n_edges_program.h"
#include "node_prog/edge_count_program.h"
#include "node_prog/edge_get_program.h"
#include "node_prog/clustering_program.h"
#include "node_prog/two_neighborhood_program.h"
#include "node_prog/traverse_with_props.h"

// size methods
uint64_t
message :: size(const db::element::element &t)
{
    uint64_t sz = size(t.get_handle()) // client handle
        + size(t.get_creat_time()) + size(t.get_del_time()) // time stamps
        + size(*t.get_props()); // properties
    return sz;
}

uint64_t
message :: size(const db::element::edge &t)
{
    uint64_t sz = size(t.base)
#ifdef WEAVER_CLDG
        + size(t.msg_count)
#endif
#ifdef WEAVER_NEW_CLDG
        + size(t.msg_count)
#endif
        + size(t.nbr);
    return sz;
}

uint64_t
message :: size(const db::element::edge* const &t)
{
    return size(*t);
}

uint64_t
message :: size(const db::element::node &t)
{
    uint64_t sz = 0;
    sz += size(t.base);
    sz += size(t.out_edges);
    sz += size(t.update_count);
#ifdef WEAVER_CLDG
    sz += size(t.msg_count);
#endif
#ifdef WEAVER_NEW_CLDG
    sz += size(t.msg_count);
#endif
    sz += size(t.already_migr);
    sz += size(t.prog_states);;
    return sz;
}

// packing methods
void message :: pack_buffer(e::buffer::packer &packer, const db::element::element &t)
{
    pack_buffer(packer, t.get_handle());
    pack_buffer(packer, t.get_creat_time());
    pack_buffer(packer, t.get_del_time());
    pack_buffer(packer, *t.get_props());
}

void message :: pack_buffer(e::buffer::packer &packer, const db::element::edge &t)
{
    pack_buffer(packer, t.base);
#ifdef WEAVER_CLDG
    pack_buffer(packer, t.msg_count);
#endif
#ifdef WEAVER_NEW_CLDG
    pack_buffer(packer, t.msg_count);
#endif
    pack_buffer(packer, t.nbr);
}

void message :: pack_buffer(e::buffer::packer &packer, const db::element::edge* const &t)
{
    pack_buffer(packer, *t);
}

void
message :: pack_buffer(e::buffer::packer &packer, const db::element::node &t)
{
    pack_buffer(packer, t.base);
    pack_buffer(packer, t.out_edges);
    pack_buffer(packer, t.update_count);
#ifdef WEAVER_CLDG
    pack_buffer(packer, t.msg_count);
#endif
#ifdef WEAVER_NEW_CLDG
    pack_buffer(packer, t.msg_count);
#endif
    pack_buffer(packer, t.already_migr);
    pack_buffer(packer, t.prog_states);
}

// unpacking methods
void
message :: unpack_buffer(e::unpacker &unpacker, db::element::element &t)
{
    std::string handle;
    vc::vclock creat_time, del_time;

    unpack_buffer(unpacker, handle);
    t.set_handle(handle);

    unpack_buffer(unpacker, creat_time);
    t.update_creat_time(creat_time);

    unpack_buffer(unpacker, del_time);
    t.update_del_time(del_time);

    unpack_buffer(unpacker, t.properties);
}

void
message :: unpack_buffer(e::unpacker &unpacker, db::element::edge &t)
{
    unpack_buffer(unpacker, t.base);
#ifdef WEAVER_CLDG
    unpack_buffer(unpacker, t.msg_count);
#endif
#ifdef WEAVER_NEW_CLDG
    unpack_buffer(unpacker, t.msg_count);
#endif
    unpack_buffer(unpacker, t.nbr);

    t.migr_edge = true; // need ack from nbr when updated
}
void
message :: unpack_buffer(e::unpacker &unpacker, db::element::edge *&t)
{
    vc::vclock temp_clk;
    edge_handle_t temp_handle("");
    node_handle_t temp_node_handle("");
    t = new db::element::edge(temp_handle, temp_clk, 0, temp_node_handle);
    unpack_buffer(unpacker, *t);
}

template <typename T>
std::shared_ptr<node_prog::Node_State_Base>
unpack_single_node_state(e::unpacker &unpacker)
{
    std::shared_ptr<T> particular_state;
    message::unpack_buffer(unpacker, particular_state);
    return std::dynamic_pointer_cast<node_prog::Node_State_Base>(particular_state);
}

void
message :: unpack_buffer(e::unpacker &unpacker, db::element::node &t)
{
    unpack_buffer(unpacker, t.base);
    unpack_buffer(unpacker, t.out_edges);
    unpack_buffer(unpacker, t.update_count);
#ifdef WEAVER_CLDG
    unpack_buffer(unpacker, t.msg_count);
#endif
#ifdef WEAVER_NEW_CLDG
    unpack_buffer(unpacker, t.msg_count);
#endif
    unpack_buffer(unpacker, t.already_migr);

    // unpack node prog state
    // need to unroll because we have to first unpack into particular state type, and then upcast and save as base type
    uint32_t num_prog_types = node_prog::END;
    assert(t.prog_states.size() == num_prog_types);

    uint32_t num_unpacked_maps;
    unpack_buffer(unpacker, num_unpacked_maps);
    assert(num_unpacked_maps == num_prog_types);

    uint64_t key;
    std::shared_ptr<node_prog::Node_State_Base> val;
    for (int i = 0; i < node_prog::END; i++) {
        db::element::node::id_to_state_t &state_map = t.prog_states[i];
        assert(state_map.size() == 0);

        uint32_t elements_left;
        unpack_buffer(unpacker, elements_left);
        state_map.reserve(elements_left);

        while (elements_left > 0) {
            unpack_buffer(unpacker, key);

            switch (i) {
                case node_prog::REACHABILITY:
                    val = unpack_single_node_state<node_prog::reach_node_state>(unpacker);
                    break;

                case node_prog::PATHLESS_REACHABILITY:
                    val = unpack_single_node_state<node_prog::pathless_reach_node_state>(unpacker);
                    break;

                case node_prog::CLUSTERING:
                    val = unpack_single_node_state<node_prog::clustering_node_state>(unpacker);
                    break;

                case node_prog::TWO_NEIGHBORHOOD:
                    val = unpack_single_node_state<node_prog::two_neighborhood_state>(unpacker);
                    break;

                case node_prog::READ_NODE_PROPS:
                    val = unpack_single_node_state<node_prog::read_node_props_state>(unpacker);
                    break;

                case node_prog::READ_EDGES_PROPS:
                    val = unpack_single_node_state<node_prog::read_edges_props_state>(unpacker);
                    break;

                case node_prog::READ_N_EDGES:
                    val = unpack_single_node_state<node_prog::read_n_edges_state>(unpacker);
                    break;

                case node_prog::EDGE_COUNT:
                    val = unpack_single_node_state<node_prog::edge_count_state>(unpacker);
                    break;

                case node_prog::EDGE_GET:
                    val = unpack_single_node_state<node_prog::edge_get_state>(unpacker);
                    break;

                case node_prog::TRAVERSE_PROPS:
                    val = unpack_single_node_state<node_prog::traverse_props_state>(unpacker);
                    break;

                default:
                    WDEBUG << "bad node prog type" << std::endl;
            }

            state_map.emplace(key, val);
            elements_left--;
        }
    }
}
