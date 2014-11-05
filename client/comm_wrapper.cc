/*
 * ===============================================================
 *    Description:  Implement client busybee wrapper.
 *
 *        Created:  2014-07-02 16:07:30
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <busybee_utils.h>

#define weaver_debug_
#include "common/weaver_constants.h"
#include "common/config_constants.h"
#include "common/message_constants.h"
#include "client/comm_wrapper.h"

using cl::comm_wrapper;

comm_wrapper :: weaver_mapper :: weaver_mapper(const configuration &config)
{
    std::vector<server> servers = config.get_servers();

    for (const server &srv: servers) {
        if (srv.type == server::VT && srv.state == server::AVAILABLE) {
            assert(mlist.find(WEAVER_TO_BUSYBEE(srv.virtual_id)) == mlist.end());
            mlist[WEAVER_TO_BUSYBEE(srv.virtual_id)] = srv.bind_to;
        }
    }
}

bool
comm_wrapper :: weaver_mapper :: lookup(uint64_t server_id, po6::net::location *loc)
{
    assert(server_id < NumVts);
    auto mlist_iter = mlist.find(WEAVER_TO_BUSYBEE(server_id));
    assert(mlist_iter != mlist.end() && "busybee mapper lookup");
    *loc = mlist_iter->second;
    return true;
}

comm_wrapper :: comm_wrapper(uint64_t bbid, const configuration &new_config)
    : config(new_config)
    , bb_gc()
    , bb_gc_ts()
    , wmap(new weaver_mapper(new_config))
    , bb(new busybee_st(&bb_gc, wmap.get(), busybee_generate_id()))
    , bb_id(bbid)
{
    bb_gc.register_thread(&bb_gc_ts);
}

comm_wrapper :: ~comm_wrapper()
{
    bb_gc.deregister_thread(&bb_gc_ts);
}

void
comm_wrapper :: reconfigure(const configuration &new_config)
{
    config = new_config;
    wmap.reset(new weaver_mapper(new_config));
    bb.reset(new busybee_st(&bb_gc, wmap.get(), busybee_generate_id()));
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
busybee_returncode
comm_wrapper :: send(uint64_t send_to, std::auto_ptr<e::buffer> msg)
{
    return bb->send(send_to, msg);
}

busybee_returncode
comm_wrapper :: recv(std::auto_ptr<e::buffer> *msg)
{
    return bb->recv(&recv_from, msg);
}
#pragma GCC diagnostic pop

void
comm_wrapper :: quiesce_thread()
{
    bb_gc.quiescent_state(&bb_gc_ts);
}

void
comm_wrapper :: drop()
{
    bb->drop(recv_from);
}
