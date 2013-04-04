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
//#include <iostream>
#include <unordered_map>
//#include <po6/net/location.h>
#include <po6/threads/mutex.h>
//#include <po6/threads/cond.h>
//#include <busybee_sta.h>

#include "common/weaver_constants.h"
#include "common/property.h"
#include "common/meta_element.h"
#include "element/node.h"
#include "element/edge.h"
#include "element/remote_node.h"

namespace std
{
    // used if we want a hash table with a remote node as the key
    template <>
    struct hash<db::element::remote_node> 
    {
        public:
            size_t operator()(db::element::remote_node x) const throw() 
            {
                return (hash<int>()(x.loc) * 6291469) + (hash<size_t>()(x.handle) * 393241); // some big primes
            }
    };
}

namespace db
{

    // Pending batched request
    class batch_request
    {
        public:
            int prev_loc; // prev server's id
            size_t dest_addr; // dest node's handle
            int dest_loc; // dest node's server id
            uint64_t coord_id; // coordinator's req id
            uint64_t prev_id; // prev server's req id
            std::vector<size_t> src_nodes;
            std::vector<size_t> parent_nodes; // pointers to parent node in traversal
            std::vector<common::property> edge_props;
            std::vector<uint64_t> vector_clock;
            std::vector<uint64_t> ignore_cache;
            uint64_t start_time;
            int num; // number of onward requests
            bool reachable; // request specific data
            std::vector<size_t> del_nodes; // deleted nodes
            std::vector<uint64_t> del_times; // delete times corr. to del_nodes
            uint32_t use_cnt; // testing

        private:
            po6::threads::mutex mutex;

        public:
            bool operator>(const batch_request &r) const;

        public:
            void lock();
            void unlock();
    };

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

    class dijkstra_queue_elem
    {
            public:
            size_t cost;
            db::element::remote_node node;
            size_t prev_node_req_id; // used for reconstructing path in coordinator

            int operator<(const dijkstra_queue_elem& other) const
            { 
                return cost > other.cost; 
            }

            dijkstra_queue_elem()
            {
            }

            dijkstra_queue_elem(size_t c, db::element::remote_node n, size_t prev)
            {
                cost = c;
                node = n;
                prev_node_req_id = prev;
            }
    };

    // Pending shorest or widest path request
    // TODO convert req_ids to uint64_t
    class dijkstra_request
    {
            public:
            uint64_t coord_id; // coordinator's req id
            size_t start_time;
            std::priority_queue<dijkstra_queue_elem> possible_next_nodes; 
            std::unordered_map<size_t, std::pair<size_t, size_t>> visited_map; // map from a node (by its create time) to its cost and the req_id of the node that came before it in the shortest path
            db::element::remote_node dest_node;
            std::vector<common::property> edge_props;
            std::vector<uint64_t> vector_clock;
            uint32_t edge_weight_name; // they key of the property which holds the weight of an an edge
            uint64_t dest_node_creat_id; // id for destination node
            bool is_widest_path;

            dijkstra_request()
            {
            }
    };

    class dijkstra_prop
    {
        public:
            size_t req_ptr, node_ptr, current_cost;
            int reply_loc;
            std::vector<common::property> edge_props;
            uint64_t start_time, coord_id;
            uint32_t edge_weight_name;
            bool is_widest_path;
    };

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
            while (!finished) {
                finished_cond.wait();
            }

            finished_lock.unlock();
        }

        // updates the delete times for a node's neighbors based on a refresh response
        void add_response(std::vector<std::pair<size_t, uint64_t>> &deleted_nodes, int from_loc)
        {
            node->update_mutex.lock();
            for (std::pair<size_t,uint64_t> &p : deleted_nodes) {
                for (db::element::edge *e : node->out_edges) {
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
} 

#endif //__REQ_OBJS__
