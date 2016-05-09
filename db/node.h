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

#ifndef weaver_db_node_h_
#define weaver_db_node_h_

#include <stdint.h>
#include <vector>
#include <unordered_map>
#include <deque>
#include <po6/threads/mutex.h>
#include <po6/threads/cond.h>

#include "node_prog/node_prog_type.h"
#include "node_prog/node.h"
#include "node_prog/edge.h"
#include "node_prog/edge_list.h"
#include "node_prog/property.h"
#include "node_prog/base_classes.h"
#include "db/types.h"
#include "db/cache_entry.h"
#include "db/element.h"
#include "db/edge.h"
#include "client/datastructures.h"

namespace message
{
    class message;
}

namespace db
{
    struct migr_data
    {
        uint64_t new_loc;
        std::vector<double> migr_score;
#ifdef WEAVER_CLDG
        std::vector<uint32_t> msg_count;
#endif
#ifdef WEAVER_NEW_CLDG
        std::vector<uint32_t> msg_count;
#endif
        bool already_migr;
        // queued requests, for the time when the node is marked in transit
        // but requests cannot yet be forwarded to new location which is still
        // setting up the node
        std::vector<std::unique_ptr<message::message>> pending_requests;

        migr_data();
    };

    class node : public node_prog::node
    {
        public:
            node(const node_handle_t &handle, uint64_t shard, vclock_ptr_t &vclk, po6::threads::mutex *mtx);
            ~node();

        public:
            enum mode
            {
                NASCENT = 0,
                STABLE,
                MOVED,
                DELETED
            };

        public:
            element base;
            uint64_t shard;
            enum mode state;
            data_map<std::vector<edge*>> out_edges;
            po6::threads::cond cv; // for locking node
            po6::threads::cond migr_cv; // make reads/writes wait while node is being migrated
            std::deque<std::pair<uint64_t, uint64_t>> tx_queue; // queued txs, identified by <vt_id, queue timestamp> tuple
            bool in_use;
            uint32_t waiters; // count of number of waiters
            bool permanently_deleted, evicted, to_evict;
            std::unique_ptr<vc::vclock> last_perm_deletion; // vclock of last edge/property permanently deleted at this node
            string_set aliases;

            // for migration
            std::unique_ptr<migr_data> migration;

            // node program cache
            std::unordered_map<cache_key_t, cache_entry> cache;
            void add_cache_value(vclock_ptr_t vc,
                std::shared_ptr<node_prog::Cache_Value_Base> cache_value,
                std::shared_ptr<std::vector<remote_node>> watch_set,
                cache_key_t key);

            // node program state
            typedef std::unordered_map<uint64_t, std::shared_ptr<node_prog::Node_State_Base>> id_to_state_t;
            typedef std::pair<node_prog::prog_type, id_to_state_t> ptype_and_map_t;
            typedef std::vector<ptype_and_map_t> prog_state_t;
            prog_state_t prog_states;
            std::unordered_map<uint64_t, std::shared_ptr<node_prog::Node_State_Base>> node_prog_states;

            // node eviction
            bool empty_evicted_node_state();
            uint32_t pending_recover_edges;
            po6::threads::mutex recover_edge_mtx;

            // fault tolerance
            std::unique_ptr<vc::vclock> last_upd_clk;
            std::unique_ptr<vc::vclock_t> restore_clk;

            // separate edge space
            std::set<int64_t> edge_ids;
            uint64_t max_edge_id;

        public:
            void add_edge_unique(edge *e); // bulk loading
            void add_edge(edge *e);
            bool edge_exists(const edge_handle_t&);
            edge& get_edge(const edge_handle_t&);
            node_prog::edge_list get_edges();
            node_prog::prop_list get_properties();
            bool has_property(std::pair<std::string, std::string> &p);
            bool has_all_properties(std::vector<std::pair<std::string, std::string>> &props);
            bool has_all_predicates(std::vector<predicate::prop_predicate> &preds);
            void set_handle(const node_handle_t &_handle) { base.set_handle(_handle); }
            const node_handle_t& get_handle() const { return base.get_handle(); }
            void add_alias(const node_handle_t &alias);
            bool del_alias(const node_handle_t &alias);
            bool is_alias(const node_handle_t &alias) const;
            void get_client_node(cl::node &n, bool, bool, bool);
    };

    using node_version_t = std::pair<node_handle_t, vclock_ptr_t>;

    struct node_version_hash
    {
        size_t operator()(const node_version_t &nv) const
        {
            return weaver_util::murmur_hasher<std::string>()(nv.first);
        }
    };

    struct evicted_node_state
    {
        std::deque<std::pair<uint64_t, uint64_t>> tx_queue;
        vc::vclock last_perm_deletion;
        node::prog_state_t prog_states;
    };
}

#endif
