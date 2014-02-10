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

#include "state/program_state.h"
#include "common/vclock.h"
#include "db/element/property.h"
#include "node_prog/property.h"

namespace message
{
    static state::program_state *prog_state;

    // size methods
    inline uint64_t size(const db::element::element &t)
    {
        uint64_t sz = sizeof(uint64_t) // id
            + size(t.get_creat_time()) + size(t.get_del_time()) // time stamps
            + size(*t.get_props()); // properties
        return sz;
    }

    inline uint64_t size(const db::element::edge &t)
    {
        uint64_t sz = size(t.base)
            + size(t.msg_count)
            + size(t.nbr);
        return sz;
    }
    inline uint64_t size(const db::element::edge* const &t)
    {
        return size(*t);
    }

    inline uint64_t size(const db::element::node &t)
    {
        uint64_t sz = size(t.base)
            + size(t.out_edges)
            + size(t.update_count)
            + size(t.msg_count)
            + size(t.already_migr)
            + prog_state->size(t.base.get_id());
        return sz;
    }

    // packing methods
    inline void pack_buffer(e::buffer::packer &packer, const db::element::element &t)
    {
        packer = packer << t.get_id();
        pack_buffer(packer, t.get_creat_time());
        pack_buffer(packer, t.get_del_time());
        pack_buffer(packer, *t.get_props());
    }

    inline void pack_buffer(e::buffer::packer &packer, const db::element::edge &t)
    {
        pack_buffer(packer, t.base);
        pack_buffer(packer, t.msg_count);
        pack_buffer(packer, t.nbr);
    }
    inline void pack_buffer(e::buffer::packer &packer, const db::element::edge* const &t)
    {
        pack_buffer(packer, *t);
    }

    inline void
    pack_buffer(e::buffer::packer &packer, const db::element::node &t)
    {
        pack_buffer(packer, t.base);
        pack_buffer(packer, t.out_edges);
        pack_buffer(packer, t.update_count);
        pack_buffer(packer, t.msg_count);
        pack_buffer(packer, t.already_migr);
        prog_state->pack(t.base.get_id(), packer);
    }

    // unpacking methods
    inline void
    unpack_buffer(e::unpacker &unpacker, db::element::element &t)
    {
        uint64_t id;
        vc::vclock creat_time, del_time;
        std::vector<db::element::property> props;

        unpacker = unpacker >> id;
        t.set_id(id);

        unpack_buffer(unpacker, creat_time);
        t.update_creat_time(creat_time);

        unpack_buffer(unpacker, del_time);
        t.update_del_time(del_time);

        unpack_buffer(unpacker, props);
        t.set_properties(props);

    }

    inline void
    unpack_buffer(e::unpacker &unpacker, db::element::edge &t)
    {
        unpack_buffer(unpacker, t.base);
        unpack_buffer(unpacker, t.msg_count);
        unpack_buffer(unpacker, t.nbr);

        t.migr_edge = true; // need ack from nbr when updated
    }
    inline void
    unpack_buffer(e::unpacker &unpacker, db::element::edge *&t)
    {
        vc::vclock junk;
        t = new db::element::edge(0, junk, 0, 0);
        unpack_buffer(unpacker, *t);
    }

    inline void
    unpack_buffer(e::unpacker &unpacker, db::element::node &t)
    {
        unpack_buffer(unpacker, t.base);
        unpack_buffer(unpacker, t.out_edges);
        unpack_buffer(unpacker, t.update_count);
        unpack_buffer(unpacker, t.msg_count);
        unpack_buffer(unpacker, t.already_migr);
        prog_state->unpack(t.base.get_id(), unpacker);
    }
}

#endif
