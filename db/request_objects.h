/*
 * ===============================================================
 *    Description:  Request objects used by the graph on a particular
                    server
 *
 *        Created:  Sunday 17 March 2013 11:00:03  EDT
 *
 *         Author:  Ayush Dubey, Greg Hill, dubey@cs.cornell.edu, gdh39@cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ================================================================
 */

#ifndef __REQ_OBJS__
#define __REQ_OBJS__

#include <vector>
//#include <fstream>
//#include <iostream>
#include <unordered_map>
//#include <po6/net/location.h>
#include <po6/threads/mutex.h>
#include <po6/threads/cond.h>
#include <busybee_sta.h>

#include "common/weaver_constants.h"
#include "common/property.h"
#include "common/meta_element.h"
#include "element/node.h"
#include "element/edge.h"

namespace db
{
    // Pending update request
    class update_request
    {
        public:
            update_request(enum message::msg_type, uint64_t, std::unique_ptr<message::message>);

        public:
            bool operator>(const update_request &r) const;

        public:
            enum message::msg_type type;
            uint64_t start_time;
            std::unique_ptr<message::message> msg;
    };

    inline
    update_request :: update_request(enum message::msg_type mt, uint64_t st, std::unique_ptr<message::message> m)
        : type(mt)
        , start_time(st)
        , msg(std::move(m))
    {
    }

    inline bool
    update_request :: operator>(const update_request &r) const
    {
        return (start_time > r.start_time);
    }

    struct req_compare
        : std::binary_function<update_request*, update_request*, bool>
    {
        bool operator()(const update_request* const &r1, const update_request* const&r2)
        {
            return (*r1 > *r2);
        }
    };

    // Pending batched request
    class batch_request
    {
        public:
            batch_request(int ploc, size_t daddr, int dloc, size_t cid, size_t pid, int myid,
                std::unique_ptr<std::vector<size_t>> nodes,
                std::unique_ptr<std::vector<common::property>> eprops,
                std::unique_ptr<std::vector<uint64_t>> vclock,
                std::unique_ptr<std::vector<size_t>> icache);
        public:
            int prev_loc; // prev server's id
            size_t dest_addr; // dest node's handle
            int dest_loc; // dest node's server id
            size_t coord_id; // coordinator's req id
            size_t prev_id; // prev server's req id
            std::unique_ptr<std::vector<size_t>> src_nodes;
            std::unique_ptr<std::vector<common::property>> edge_props;
            std::unique_ptr<std::vector<uint64_t>> vector_clock;
            std::unique_ptr<std::vector<size_t>> ignore_cache;
            uint64_t start_time;
            int num; // number of onward requests
            bool reachable; // request specific data
            std::unique_ptr<std::vector<size_t>> del_nodes; // deleted nodes
            std::unique_ptr<std::vector<uint64_t>> del_times; // delete times corr. to del_nodes
            uint32_t use_cnt; // testing

        private:
            po6::threads::mutex mutex;

        public:
            bool operator>(const batch_request &r) const;

        public:
            void lock();
            void unlock();
    };

    inline
    batch_request :: batch_request(int ploc, size_t daddr, int dloc, size_t cid, size_t pid, int myid,
        std::unique_ptr<std::vector<size_t>> nodes,
        std::unique_ptr<std::vector<common::property>> eprops,
        std::unique_ptr<std::vector<uint64_t>> vclock,
        std::unique_ptr<std::vector<size_t>> icache)
        : prev_loc(ploc)
        , dest_addr(daddr)
        , dest_loc(dloc)
        , coord_id(cid)
        , prev_id(pid)
        , src_nodes(std::move(nodes))
        , edge_props(std::move(eprops))
        , vector_clock(std::move(vclock))
        , ignore_cache(std::move(icache))
        , num(0)
        , reachable(false)
        , use_cnt(0)
    {
        start_time = vector_clock->at(myid);
    }

    inline bool
    batch_request :: operator>(const batch_request &r) const
    {
        return (coord_id > r.coord_id);
    }

    inline void
    batch_request :: lock()
    {
        mutex.lock();
        use_cnt++;
    }

    inline void
    batch_request :: unlock()
    {
        use_cnt--;
        mutex.unlock();
    }

    /*
    // Pending clustering request
    class clustering_request
    {
        public:
            int coordinator_loc;
            size_t id; // coordinator's req id
            // key is shard, value is set of neighbors on that shard
            std::unordered_map<int, std::unordered_set<size_t>> nbrs; 
            size_t edges; // numerator in calculation
            size_t possible_edges; // denominator in calculation
            size_t responses_left; // 1 response per shard with neighbor
            po6::threads::mutex mutex;

        clustering_request()
        {
            edges = 0;
        }
    };


    // Pending refresh request
    class refresh_request
    {
        public:
            refresh_request(size_t num_shards, db::element::node *n);
        private:
            po6::threads::mutex finished_lock;
            po6::threads::cond finished_cond;
        public:
            db::element::node *node;
            size_t responses_left; // 1 response per shard with neighbor
            bool finished;


        void wait_on_responses()
        {
            finished_lock.lock();
            while (!finished)
            {
                finished_cond.wait();
            }

            finished_lock.unlock();
        }

        // updates the delete times for a node's neighbors based on a refresh response
        void add_response(std::vector<std::pair<size_t, uint64_t>> &deleted_nodes, int from_loc)
        {
            node->update_mutex.lock();
            for (std::pair<size_t,uint64_t> &p : deleted_nodes)
            {
                for (db::element::edge *e : node->out_edges)
                {
                    if (from_loc == e->nbr->get_loc() && p.first == (size_t) e->nbr->get_addr()) {
                        e->nbr->update_del_time(p.second);
                    }
                }
            }
            node->update_mutex.unlock();
            finished_lock.lock();
            responses_left--;
            if (responses_left == 0) {
                finished =  true;
                finished_cond.broadcast();
            }
            finished_lock.unlock();
        }
    };

    refresh_request :: refresh_request(size_t num_shards, db::element::node * n)
        : finished_cond(&finished_lock)
        , node(n)
        , responses_left(num_shards)
        , finished(false)
    {
    }
    */

    class migrate_request
    {
        public:
            po6::threads::mutex mutex;
            element::node *cur_node; // node being migrated
            int new_loc; // shard to which this node is being migrated
            size_t migr_node; // node handle on new shard
            std::vector<std::unique_ptr<message::message>> pending_updates; // queued updates
            std::vector<size_t> pending_update_ids; // ids of queued updates
            uint32_t num_pending_updates;
            std::vector<std::shared_ptr<batch_request>> pending_requests; // queued requests
            uint64_t my_clock;
    };

} //namespace db

#endif //__REQ_OBJS__
