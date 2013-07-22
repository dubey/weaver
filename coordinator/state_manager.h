/*
 * ===============================================================
 *    Description:  Common state for the coordinator group, stored
 *                  in a transactional key-value store
 *
 *        Created:  07/12/2013 11:37:01 AM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __COORD_STATE_MANAGER__
#define __COORD_STATE_MANAGER__

#include <unordered_map>

#include "common/weaver_constants.h"
#include "common/meta_element.h"
#include "common/busybee_infra.h"
#include "common/message.h"
#include "threadpool/threadpool.h"

namespace coordinator
{
    class state_manager
    {
        public:
            state_manager();

        public:
            thread::pool thread_pool;
            busybee_mta *bb;

        private:
            std::shared_ptr<po6::net::location> myloc;
            po6::threads::mutex mutex;
            std::unordered_map<uint64_t, common::meta_element> elem_locs;

        public:
            common::meta_element check_and_get_loc(uint64_t elem);
            busybee_returncode send(uint64_t shard_id, std::auto_ptr<e::buffer> buf);
    };

    inline
    state_manager :: state_manager()
        : thread_pool(NUM_THREADS)
    {
        initialize_busybee(bb, COORD_SM_ID, myloc);
    }

    inline common::meta_element
    state_manager :: check_and_get_loc(uint64_t elem)
    {
        common::meta_element ret(-1);
        mutex.lock();
        if (elem_locs.find(elem) != elem_locs.end()) {
            ret = elem_locs.at(elem);
        }
        mutex.unlock();
        return ret;
    }

    inline busybee_returncode
    state_manager :: send(uint64_t shard_id, std::auto_ptr<e::buffer> buf)
    {
        busybee_returncode ret;
        if ((ret = bb->send(shard_id, buf)) != BUSYBEE_SUCCESS) {
            DEBUG << "message sending error: " << ret << std::endl;
        }
        return ret;
    }
}

#endif
