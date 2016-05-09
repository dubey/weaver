/*
 * ===============================================================
 *    Description:  Node program running state
 *
 *        Created:  Sunday 17 March 2013 11:00:03  EDT
 *
 *         Author:  Ayush Dubey, Greg Hill
 *                  dubey@cs.cornell.edu, gdh39@cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ================================================================
 */

#ifndef weaver_db_node_prog_running_state_
#define weaver_db_node_prog_running_state_

#include <vector>
#include <deque>
#include <unordered_map>

#include "common/vclock.h"
#include "node_prog/base_classes.h"

namespace db
{
    struct node_prog_running_state
    {
        node_prog_running_state() : m_handle(nullptr) { }

        std::string m_type;
        void *m_handle;
        uint64_t vt_id;
        std::shared_ptr<vc::vclock> req_vclock;
        uint64_t req_id;
        uint64_t vt_prog_ptr;
        std::deque<std::pair<node_handle_t, np_param_ptr_t>> start_node_params;
        //std::unique_ptr<cache_response<CacheValueType>> cache_value;
        std::unordered_map<uint64_t, std::deque<std::pair<node_handle_t, np_param_ptr_t>>> batched_node_progs;
        std::vector<std::pair<node_handle_t, vclock_ptr_t>> nodes_that_created_state;
   };
}

#endif
