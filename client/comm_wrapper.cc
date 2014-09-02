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
    std::vector<std::pair<server_id, po6::net::location>> addresses;
    config.get_all_addresses(&addresses);

    for (auto &p: addresses) {
        assert(config.get_weaver_id(p.first) != UINT64_MAX);
        uint64_t factor = config.get_type(p.first) == server::SHARD ? 1 : 0;
        uint64_t wid = config.get_weaver_id(p.first) + NumVts*factor;
        if (config.get_state(p.first) == server::AVAILABLE) {
            assert(mlist.find(WEAVER_TO_BUSYBEE(wid)) == mlist.end());
            mlist[WEAVER_TO_BUSYBEE(wid)] = p.second;
        }

        server::state_t st = config.get_state(p.first);
        if (st != server::AVAILABLE) {
            WDEBUG << "Server " << wid << " is in trouble, has state " << server::to_string(st) << std::endl;
        } else {
            WDEBUG << "Server " << wid << " is healthy, has state " << server::to_string(st) << std::endl;
        }
    }
}

bool
comm_wrapper :: weaver_mapper :: lookup(uint64_t server_id, po6::net::location *loc)
{
    auto mlist_iter = mlist.find(WEAVER_TO_BUSYBEE(server_id));
    if (mlist_iter == mlist.end()) {
        WDEBUG << "busybee map lookup failed for orig id " << server_id
               << ", busybee id " << WEAVER_TO_BUSYBEE(server_id) << std::endl;
    }
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

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
busybee_returncode
comm_wrapper :: send(uint64_t send_to, std::auto_ptr<e::buffer> msg)
{
    busybee_returncode code = bb->send(send_to, msg);
    if (code != BUSYBEE_SUCCESS) {
        WDEBUG << "busybee send returned " << code << std::endl;
    }
    return code;
}

busybee_returncode
comm_wrapper :: recv(std::auto_ptr<e::buffer> *msg)
{
    uint64_t recv_from;
    return bb->recv(&recv_from, msg);
}
#pragma GCC diagnostic pop

void
comm_wrapper :: quiesce_thread()
{
    bb_gc.quiescent_state(&bb_gc_ts);
}
