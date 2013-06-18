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
    inline size_t size(const db::element::edge* const &t)
    {
        size_t sz = 2*sizeof(uint64_t) + // time stamps
            size(*t->get_props()) + // properties
            size(t->nbr);
        return sz;
    }

    inline size_t size(const db::element::node &t)
    {
        size_t sz = 2*sizeof(uint64_t) // time stamps
            + size(*t.get_props())  // properties
            + size(t.out_edges)
            + size(t.in_edges)
            + size(t.update_count)
            + size(t.prev_locs)
            + size(t.agg_msg_count)
            + prog_state->size(t.get_creat_time());
        return sz;
    }

    // packing methods
    inline void pack_buffer(e::buffer::packer &packer, const db::element::edge* const &t)
    {
        packer = packer << t->get_creat_time() << t->get_del_time();
        pack_buffer(packer, *t->get_props());
        pack_buffer(packer, t->nbr);
    }

    inline void
    pack_buffer(e::buffer::packer &packer, const db::element::node &t)
    {
        packer = packer << t.get_creat_time() << t.get_del_time();
        pack_buffer(packer, *t.get_props());
        pack_buffer(packer, t.out_edges);
        pack_buffer(packer, t.in_edges);
        pack_buffer(packer, t.update_count);
        pack_buffer(packer, t.prev_locs);
        pack_buffer(packer, t.agg_msg_count);
        DEBUG << "packing node " << t.get_creat_time() << std::endl;
        prog_state->pack(t.get_creat_time(), packer);
    }

    // unpacking methods
    inline void
    unpack_buffer(e::unpacker &unpacker, db::element::edge *&t)
    {
        uint64_t tc, td;
        std::vector<common::property> props;
        db::element::remote_node rn;
        unpacker = unpacker >> tc >> td;
        unpack_buffer(unpacker, props);
        unpack_buffer(unpacker, rn);
        t = new db::element::edge(tc, rn);
        t->update_del_time(td);
        t->set_properties(props);
    }

    inline void
    unpack_buffer(e::unpacker &unpacker, db::element::node &t)
    {
        uint64_t tc, td;
        std::vector<common::property> props;
        unpacker = unpacker >> tc >> td;
        unpack_buffer(unpacker, props);
        unpack_buffer(unpacker, t.out_edges);
        unpack_buffer(unpacker, t.in_edges);
        unpack_buffer(unpacker, t.update_count);
        unpack_buffer(unpacker, t.prev_locs);
        unpack_buffer(unpacker, t.agg_msg_count);
        t.update_creat_time(tc);
        t.update_del_time(td);
        t.set_properties(props);
        DEBUG << "unpacking node " << t.get_creat_time() << std::endl;
        prog_state->unpack(tc, unpacker);
    }
}

#endif
