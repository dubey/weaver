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

#ifndef __MESSAGE_GRAPH_ELEM__
#define __MESSAGE_GRAPH_ELEM__

#include "message.h"
#include "state/program_state.h"

namespace message
{
    static state::program_state *prog_state;

    // size methods
    inline uint64_t size(const db::element::edge* const &t)
    {
        uint64_t sz = sizeof(uint64_t) // handle
            + size(t->get_creat_time()) + size(t->get_del_time()) // time stamps
            + size(*t->get_props()) // properties
            + size(t->nbr);
        return sz;
    }

    inline uint64_t size(const db::element::node &t)
    {
        uint64_t sz = sizeof(uint64_t) // handle
            + size(t.get_creat_time()) + size(t.get_del_time()) // time stamps
            + size(*t.get_props())  // properties
            + size(t.out_edges)
            + size(t.update_count)
            + size(t.prev_locs)
            + size(t.agg_msg_count)
            + size(t.msg_count)
            + prog_state->size(t.get_handle());
        return sz;
    }

    // packing methods
    inline void pack_buffer(e::buffer::packer &packer, const db::element::edge* const &t)
    {
        packer = packer << t->get_handle();
        pack_buffer(packer, t->get_creat_time());
        pack_buffer(packer, t->get_del_time());
        pack_buffer(packer, *t->get_props());
        pack_buffer(packer, t->nbr);
    }

    inline void
    pack_buffer(e::buffer::packer &packer, const db::element::node &t)
    {
        packer = packer << t.get_handle();
        pack_buffer(packer, t.get_creat_time());
        pack_buffer(packer, t.get_del_time());
        pack_buffer(packer, *t.get_props());
        pack_buffer(packer, t.out_edges);
        pack_buffer(packer, t.update_count);
        pack_buffer(packer, t.prev_locs);
        pack_buffer(packer, t.agg_msg_count);
        pack_buffer(packer, t.msg_count);
        prog_state->pack(t.get_handle(), packer);
    }

    // unpacking methods
    inline void
    unpack_buffer(e::unpacker &unpacker, db::element::edge *&t)
    {
        uint64_t handle;
        vc::vclock creat_time, del_time;
        std::vector<common::property> props;
        db::element::remote_node rn;
        unpacker = unpacker >> handle;
        unpack_buffer(unpacker, creat_time);
        unpack_buffer(unpacker, del_time);
        unpack_buffer(unpacker, props);
        unpack_buffer(unpacker, rn);
        t = new db::element::edge(handle, creat_time, rn);
        t->update_del_time(del_time);
        t->set_properties(props);
        t->migr_edge = true; // need ack from nbr when updated
    }

    inline void
    unpack_buffer(e::unpacker &unpacker, db::element::node &t)
    {
        uint64_t handle;
        vc::vclock creat_time, del_time;
        std::vector<common::property> props;
        unpacker = unpacker >> handle;
        unpack_buffer(unpacker, creat_time);
        unpack_buffer(unpacker, del_time);
        unpack_buffer(unpacker, props);
        unpack_buffer(unpacker, t.out_edges);
        unpack_buffer(unpacker, t.update_count);
        unpack_buffer(unpacker, t.prev_locs);
        unpack_buffer(unpacker, t.agg_msg_count);
        unpack_buffer(unpacker, t.msg_count);
        t.set_handle(handle);
        t.update_creat_time(creat_time);
        t.update_del_time(del_time);
        t.set_properties(props);
        prog_state->unpack(handle, unpacker);
    }
}

#endif
