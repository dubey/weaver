/*
 * ===============================================================
 *    Description: Graph node class 
 *
 *        Created:  Tuesday 16 October 2012 02:24:02  EDT
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 * 
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_db_element_node_h_
#define weaver_db_element_node_h_

#include <stdint.h>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <deque>
#include <po6/threads/mutex.h>
#include <po6/threads/cond.h>

#include "node_prog/node.h"
#include "node_prog/edge.h"
#include "node_prog/edge_list.h"
#include "node_prog/property.h"
#include "node_prog/base_classes.h"
#include "db/cache_entry.h"
#include "db/element.h"
#include "db/edge.h"
#include "db/shard_constants.h"

namespace message
{
    class message;
}

namespace db
{
namespace element
{
    class node : public node_prog::node
    {
        public:
            node(const node_handle_t &handle, vc::vclock &vclk, po6::threads::mutex *mtx);
            ~node() { }

        public:
            enum mode
            {
                NASCENT = 0,
                STABLE,
                MOVED
            };

        public:
            element base;
            enum mode state;
            std::unordered_map<edge_handle_t, edge*> out_edges;
            po6::threads::cond cv; // for locking node
            po6::threads::cond migr_cv; // make reads/writes wait while node is being migrated
            std::deque<std::pair<uint64_t, uint64_t>> tx_queue; // queued txs, identified by <vt_id, queue timestamp> tuple
            bool in_use;
            uint32_t waiters; // count of number of waiters
            bool permanently_deleted;
            std::unique_ptr<vc::vclock> last_perm_deletion; // vclock of last edge/property permanently deleted at this node

            // for migration
            uint64_t new_loc;
            uint64_t update_count;
            std::vector<double> migr_score;
#ifdef WEAVER_CLDG
            std::vector<uint32_t> msg_count;
#endif
#ifdef WEAVER_NEW_CLDG
            std::vector<uint32_t> msg_count;
#endif
            bool updated, already_migr;
            uint32_t dependent_del;
            // queued requests, for the time when the node is marked in transit
            // but requests cannot yet be forwarded to new location which is still
            // setting up the node
            std::vector<std::unique_ptr<message::message>> pending_requests;

            // node program cache
            std::unordered_map<cache_key_t, cache_entry> cache;
            void add_cache_value(std::shared_ptr<vc::vclock> vc,
                std::shared_ptr<node_prog::Cache_Value_Base> cache_value,
                std::shared_ptr<std::vector<remote_node>> watch_set,
                cache_key_t key);

            // node program state
            typedef std::unordered_map<uint64_t, std::shared_ptr<node_prog::Node_State_Base>> id_to_state_t;
            typedef std::vector<id_to_state_t> prog_state_t;
            prog_state_t prog_states;

            // fault tolerance
            vc::vclock last_upd_clk;
            vc::vclock_t restore_clk;

        public:
            void add_edge(edge *e);
            node_prog::edge_list get_edges();
            node_prog::prop_list get_properties();
            bool has_property(std::pair<std::string, std::string> &p);
            bool has_all_properties(std::vector<std::pair<std::string, std::string>> &props);
            void set_handle(const node_handle_t &_handle) { base.set_handle(_handle); }
            node_handle_t get_handle() const { return base.get_handle(); }
    };
}
}

#endif
