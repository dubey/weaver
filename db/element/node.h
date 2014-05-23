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
#include <queue>
#include <deque>
#include <po6/threads/mutex.h>
#include <po6/threads/cond.h>

#include "node_prog/node.h"
#include "node_prog/edge.h"
#include "node_prog/edge_list.h"
#include "node_prog/property.h"
#include "node_prog/base_classes.h"
#include "common/weaver_constants.h"
#include "db/cache/prog_cache.h"
#include "element.h"
#include "edge.h"

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
            node(uint64_t id, vc::vclock &vclk, po6::threads::mutex *mtx);
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
            std::unordered_map<uint64_t, edge*> out_edges;
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
            std::vector<uint32_t> msg_count;
            bool updated, already_migr;
            uint32_t dependent_del;
            // queued requests, for the time when the node is marked in transit
            // but requests cannot yet be forwarded to new location which is still
            // setting up the node
            std::vector<std::unique_ptr<message::message>> pending_requests;

            // for node prog caching
            db::caching::program_cache cache;

            // node program state
            typedef std::unordered_map<uint64_t, std::shared_ptr<node_prog::Node_State_Base>> id_to_state_t;
            typedef std::vector<id_to_state_t> prog_state_t;
            prog_state_t prog_states;

#ifdef weaver_debug_
            // testing
            std::unordered_set<uint64_t> edge_handles;
#endif

        public:
            void add_edge(edge *e);

            node_prog::edge_list get_edges()
            {
                assert(base.view_time != NULL);
                return node_prog::edge_list(out_edges, base.view_time);
            };

            node_prog::prop_list get_properties()
            {
                assert(base.view_time != NULL);
                return node_prog::prop_list(base.properties, *base.view_time);
            };
    };

    inline
    node :: node(uint64_t id, vc::vclock &vclk, po6::threads::mutex *mtx)
        : base(id, vclk)
        , state(mode::NASCENT)
        , cv(mtx)
        , migr_cv(mtx)
        , in_use(true)
        , waiters(0)
        , permanently_deleted(false)
        , last_perm_deletion(nullptr)
        , new_loc(UINT64_MAX)
        , update_count(1)
        , migr_score(NUM_SHARDS, 0)
        , updated(true)
        , already_migr(false)
        , dependent_del(0)
        , cache()
    {
        int num_prog_types = node_prog::END;
        prog_states.resize(num_prog_types);
    }

    inline void
    node :: add_edge(edge *e)
    {
#ifdef weaver_debug_
        edge_handles.emplace(e->get_id());
#endif
        out_edges.emplace(e->get_id(), e);
    }

    inline bool
    compare_msg_cnt(const node *n1, const node *n2)
    {
        uint64_t s1 = 0;
        uint64_t s2 = 0;
        for (uint32_t i: n1->msg_count) {
            s1 += (uint64_t)i;
        }
        for (uint32_t i: n2->msg_count) {
            s2 += (uint64_t)i;
        }
        return (s1<s2);
    }

}
}

#endif
