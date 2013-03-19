/*
 * ===============================================================
 *    Description:  The part of a graph stored on a particular 
                    server
 *
 *        Created:  Tuesday 16 October 2012 11:00:03  EDT
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ================================================================
 */

#ifndef __GRAPH__
#define __GRAPH__

#include <stdlib.h>
#include <vector>
#include <fstream>
#include <iostream>
#include <unordered_map>
#include <po6/net/location.h>
#include <po6/threads/mutex.h>
#include <po6/threads/cond.h>
#include <busybee_sta.h>

#include "common/weaver_constants.h"
#include "common/property.h"
#include "common/meta_element.h"
#include "element/node.h"
#include "element/edge.h"
#include "cache/cache.h"
#include "threadpool/threadpool.h"

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
        /*
            batch_request(int ploc, size_t daddr, int dloc, size_t cid, size_t pid, int myid,
                std::unique_ptr<std::vector<size_t>> nodes,
                std::shared_ptr<std::vector<common::property>> eprops,
                std::unique_ptr<std::vector<uint64_t>> vclock,
                std::unique_ptr<std::vector<size_t>> icache);
            */
            batch_request();
        public:
            int prev_loc; // prev server's id
            size_t dest_addr; // dest node's handle
            int dest_loc; // dest node's server id
            size_t coord_id; // coordinator's req id
            size_t prev_id; // prev server's req id
            /*
            std::unique_ptr<std::vector<size_t>> src_nodes;
            std::unique_ptr<std::vector<size_t>> parent_nodes; // pointers to parent node in traversal
            std::shared_ptr<std::vector<common::property>> edge_props;
            std::unique_ptr<std::vector<uint64_t>> vector_clock;
            std::unique_ptr<std::vector<size_t>> ignore_cache;
            */
            std::vector<size_t> src_nodes;
            std::vector<size_t> parent_nodes; // pointers to parent node in traversal
            std::vector<common::property> edge_props;
            std::vector<uint64_t> vector_clock;
            std::vector<size_t> ignore_cache;

            uint64_t start_time;
            int num; // number of onward requests
            bool reachable; // request specific data
            /*
            std::unique_ptr<std::vector<size_t>> del_nodes; // deleted nodes
            std::unique_ptr<std::vector<uint64_t>> del_times; // delete times corr. to del_nodes
            */
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

    inline
    batch_request :: batch_request()
    {
    }

    /*
    inline
    batch_request :: batch_request(int ploc, size_t daddr, int dloc, size_t cid, size_t pid, int myid,
        std::unique_ptr<std::vector<size_t>> nodes,
        std::shared_ptr<std::vector<common::property>> eprops,
        std::unique_ptr<std::vector<uint64_t>> vclock,
        std::unique_ptr<std::vector<size_t>> icache)
        : prev_loc(ploc)
        , dest_addr(daddr)
        , dest_loc(dloc)
        , coord_id(cid)
        , prev_id(pid)
        , src_nodes(std::move(nodes))
        , parent_nodes(new std::vector<size_t>(src_nodes->size(), UINT64_MAX))
        , edge_props(std::move(eprops))
        , vector_clock(std::move(vclock))
        , ignore_cache(std::move(icache))
        , num(0)
        , reachable(false)
        , use_cnt(0)
    {
        start_time = vector_clock->at(myid);
    }
    */

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

    class graph
    {
        public:
            graph(int my_id, const char* ip_addr, int port);

        private:
            po6::threads::mutex update_mutex, request_mutex;
            uint64_t my_clock;

        public:
            std::vector<element::node*> nodes;
            int node_count;
            int num_shards;
            std::shared_ptr<po6::net::location> myloc;
            std::shared_ptr<po6::net::location> coord;
            po6::net::location **shard_locs; // po6 locations for each shard
            cache::reach_cache cache; // reachability cache
            busybee_sta bb; // Busybee instance used for sending messages
            busybee_sta bb_recv; // Busybee instance used for receiving messages
            po6::threads::mutex bb_lock; // Busybee lock
            int myid;
            // For traversal
            size_t outgoing_req_id_counter;
            po6::threads::mutex outgoing_req_id_counter_mutex, visited_mutex;
            std::unordered_map<size_t, std::shared_ptr<batch_request>> pending_batch;
            // TODO need clean up of old done requests
            std::unordered_set<size_t> done_requests; // requests which need to be killed
            db::thread::pool thread_pool;
            std::unordered_map<size_t, std::vector<size_t>> visit_map_odd, visit_map_even; // visited ids -> nodes
            bool visit_map;
            // For node migration
            migrate_request mrequest; // pending migration request object
            po6::threads::mutex migration_mutex;
            element::node *migr_node; // newly migrated node
            std::priority_queue<update_request*, std::vector<update_request*>, req_compare> pending_updates;
            std::deque<size_t> pending_update_ids;
            uint64_t target_clock;
            // testing
            std::unordered_map<size_t, uint64_t> req_count;
            bool already_migrated;
            
        public:
            bool check_clock(uint64_t time);
            element::node* create_node(uint64_t time, bool migrate);
            std::pair<bool, size_t> create_edge(size_t n1, uint64_t time, size_t n2, int loc2, uint64_t tc2);
            bool create_reverse_edge(uint64_t time, size_t local_node, size_t remote_node, int remote_loc);
            std::pair<bool, std::unique_ptr<std::vector<size_t>>> delete_node(element::node *n, uint64_t del_time);
            std::pair<bool, std::unique_ptr<std::vector<size_t>>> delete_edge(element::node *n, size_t edge_handle, uint64_t del_time);
            void refresh_edge(element::node *n, element::edge *e, uint64_t del_time);
            bool add_edge_property(size_t n, size_t e, common::property &prop);
            std::pair<bool, std::unique_ptr<std::vector<size_t>>> delete_all_edge_property(size_t n, size_t e, uint32_t key, uint64_t time);
            void update_migrated_nbr(size_t lnode, size_t orig_node, int orig_loc, size_t new_node, int new_loc);
            bool check_request(size_t req_id);
            void add_done_request(size_t req_id);
            void broadcast_done_request(size_t req_id); 
            bool mark_visited(element::node *n, size_t req_counter);
            void remove_visited(element::node *n, size_t req_counter);
            void record_visited(size_t coord_req_id, const std::vector<size_t>& nodes);
            size_t get_cache(size_t local_node, size_t dest_loc, size_t dest_node,
                std::vector<common::property>& edge_props);
            void add_cache(size_t local_node, size_t dest_loc, size_t dest_node, size_t req_i,
                std::vector<common::property>&  edge_props);
            void transient_add_cache(size_t local_node, size_t dest_loc, size_t dest_node, size_t req_id,
                std::vector<common::property>& edge_props);
            void remove_cache(size_t req_id, element::node *);
            void commit_cache(size_t req_id);
            busybee_returncode send(po6::net::location loc, std::auto_ptr<e::buffer> buf);
            busybee_returncode send(std::unique_ptr<po6::net::location> loc, std::auto_ptr<e::buffer> buf);
            busybee_returncode send(po6::net::location *loc, std::auto_ptr<e::buffer> buf);
            busybee_returncode send(int loc, std::auto_ptr<e::buffer> buf);
            busybee_returncode send_coord(std::auto_ptr<e::buffer> buf);
            void wait_for_updates(uint64_t recd_clock);
            void sync_clocks();
            void propagate_request(std::vector<size_t> &nodes, std::shared_ptr<batch_request> request, int prop_loc);
            void queue_transit_node_update(size_t req_id, std::unique_ptr<message::message> msg);
            bool set_target_clock(uint64_t time);
            void set_update_ids(std::vector<size_t>&);
            bool increment_clock();
            // testing
            bool migrate_test();
            void sort_and_print_nodes();
    };

    inline
    graph :: graph(int my_id, const char* ip_addr, int port)
        : my_clock(0)
        , node_count(0)
        , num_shards(NUM_SHARDS)
        , myloc(new po6::net::location(ip_addr, port))
        , coord(new po6::net::location(COORD_IPADDR, COORD_REC_PORT))
        , cache(NUM_SHARDS)
        , bb(myloc->address, myloc->port + SEND_PORT_INCR, 0)
        , bb_recv(myloc->address, myloc->port, 0)
        , myid(my_id)
        , outgoing_req_id_counter(0)
        , thread_pool(NUM_THREADS)
        , visit_map(true)
        , target_clock(0)
        , already_migrated(false)
    {
        int i, inport;
        po6::net::location *temp_loc;
        std::string ipaddr;
        std::ifstream file(SHARDS_DESC_FILE);
        shard_locs = (po6::net::location **)malloc(sizeof(po6::net::location *) * num_shards);
        i = 0;
        while (file >> ipaddr >> inport)
        {
            temp_loc = new po6::net::location(ipaddr.c_str(), inport);
            shard_locs[i] = temp_loc;
            ++i;
        }
        file.close();
    }

    inline bool
    graph :: increment_clock()
    {
        bool check;
        update_mutex.lock();
        my_clock++;
        check = (my_clock == target_clock);
        if (check) {
            target_clock = 0;
        }
        update_mutex.unlock();
        thread_pool.queue_mutex.lock();
        thread_pool.traversal_queue_cond.broadcast();
        thread_pool.update_queue_cond.broadcast();
        thread_pool.queue_mutex.unlock();
        return check;
    }

    inline void
    graph :: queue_transit_node_update(size_t req_id, std::unique_ptr<message::message> msg)
    {
        mrequest.mutex.lock();
        mrequest.pending_updates.emplace_back(std::move(msg));
        mrequest.pending_update_ids.emplace_back(req_id);
        mrequest.mutex.unlock();
    }

    inline bool
    graph :: set_target_clock(uint64_t time)
    {
        bool ret;
        update_mutex.lock();
        if (time == my_clock) {
            ret = true;
        } else {
            target_clock = time;
            ret = false;
        }
        update_mutex.unlock();
        return ret;
    }

    inline void
    graph :: set_update_ids(std::vector<size_t> &update_ids)
    {
        migration_mutex.lock();
        for (size_t id: update_ids)
        {
            pending_update_ids.push_back(id);
        }
        migration_mutex.unlock();
    }

    inline bool
    graph :: check_clock(uint64_t time)
    {
        bool ret;
        update_mutex.lock();
        ret = (time <= my_clock);
        update_mutex.unlock();
        return ret;
    }

    inline element::node*
    graph :: create_node(uint64_t time, bool migrate = false)
    {
        element::node *new_node = new element::node(time);
        if (migrate) {
            migration_mutex.lock();
            migr_node = new_node;
            migration_mutex.unlock();
        } else {
            new_node->state = element::node::mode::STABLE;
            //increment_clock();
        }
        update_mutex.lock();
        nodes.emplace_back(new_node);
        update_mutex.unlock();
#ifdef DEBUG
        std::cout << "Creating node, addr = " << (void*) new_node 
                << " and node count " << (++node_count) << std::endl;
#endif
        return new_node;
    }

    inline std::pair<bool, std::unique_ptr<std::vector<size_t>>>
    graph :: delete_node(element::node *n, uint64_t del_time)
    {
        std::pair<bool, std::unique_ptr<std::vector<size_t>>> ret;
        n->update_mutex.lock();
        if (n->state == element::node::mode::IN_TRANSIT) {
            ret.first = false;
        } else {
            n->update_del_time(del_time);
            ret.first = true;
        }
        ret.second = std::move(n->purge_cache());
        for (auto rid: *ret.second)
        {
            remove_cache(rid, n);
        }
        n->update_mutex.unlock();
        return ret;
    }

    inline std::pair<bool, size_t>
    graph :: create_edge(size_t n1, uint64_t time, size_t n2, int loc2, uint64_t tc2)
    {
        std::pair<bool, size_t> ret;
        element::node *local_node = (element::node *)n1;
        local_node->update_mutex.lock();
        if (local_node->state == element::node::mode::IN_TRANSIT) {
            local_node->update_mutex.unlock();
            ret.first = false;
        } else {
            element::edge *new_edge = new element::edge(time, loc2, n2);
            ret.second = local_node->add_edge(new_edge, true);
            local_node->update_mutex.unlock();
            message::message msg(message::REVERSE_EDGE_CREATE);
            message::prepare_message(msg, message::REVERSE_EDGE_CREATE, tc2, time, n1, myid, n2);
            send(loc2, msg.buf);
#ifdef DEBUG
            std::cout << "Creating edge, addr = " << (void *) new_edge << std::endl;
#endif
            ret.first = true;
        }
        return ret;
    }

    inline bool
    graph :: create_reverse_edge(uint64_t time, size_t local_node, size_t remote_node, int remote_loc)
    {
        bool ret;
        element::node *n = (element::node *)local_node;
        n->update_mutex.lock();
        if (n->state == element::node::mode::IN_TRANSIT) {
            ret = false;
        } else {
            element::edge *new_edge = new element::edge(time, remote_loc, remote_node);
            n->add_edge(new_edge, false);
#ifdef DEBUG
            std::cout << "New rev edge: " << (void*)new_edge->nbr.handle << " " << new_edge->nbr.loc << " at lnode " << (void*)local_node << std::endl;
            std::cout << " in edge size " << n->in_edges.size() << std::endl;
#endif
            ret = true;
        }
        n->update_mutex.unlock();
        return ret;
    }

    inline std::pair<bool, std::unique_ptr<std::vector<size_t>>>
    graph :: delete_edge(element::node *n, size_t edge_handle, uint64_t del_time)
    {
        std::pair<bool, std::unique_ptr<std::vector<size_t>>> ret;
        n->update_mutex.lock();
        if (n->state == element::node::mode::IN_TRANSIT) {
            ret.first = false;
        } else {
            element::edge *e = n->out_edges.at(edge_handle);
            e->update_del_time(del_time);
            ret.first = true;
        }
        ret.second = std::move(n->purge_cache());
        for (auto rid: *ret.second)
        {
            remove_cache(rid, n);
        }
        n->update_mutex.unlock();
        return ret;
    }

    inline void
    graph :: refresh_edge(element::node *n, element::edge *e, uint64_t del_time)
    {
        // TODO caching and edge refreshes
        n->update_mutex.lock();
        e->update_del_time(del_time);
        n->update_mutex.unlock();
    }

    inline bool
    graph :: add_edge_property(size_t node, size_t edge, common::property &prop)
    {
        element::node *n = (element::node*)node;
        element::edge *e = n->out_edges.at(edge);
        bool ret;
        n->update_mutex.lock();
        if (n->state == element::node::mode::IN_TRANSIT) {
            ret = false;
        } else {
            e->add_property(prop);
            ret = true;
        }
        n->update_mutex.unlock();
        return ret;
    }

    inline std::pair<bool, std::unique_ptr<std::vector<size_t>>>
    graph :: delete_all_edge_property(size_t node, size_t edge, uint32_t key, uint64_t time)
    {
        element::node *n = (element::node*)node;
        element::edge *e = n->out_edges.at(edge);
        std::pair<bool, std::unique_ptr<std::vector<size_t>>> ret;
        n->update_mutex.lock();
        if (n->state == element::node::mode::IN_TRANSIT) {
            ret.first = false;
        } else {
            e->delete_property(key, time);
            ret.first = true;
        }
        ret.second = std::move(n->purge_cache());
        for (auto rid: *ret.second)
        {
            remove_cache(rid, n);
        }
        n->update_mutex.unlock();
        return ret;
    }

    inline void
    graph :: update_migrated_nbr(size_t lnode, size_t orig_node, int orig_loc, size_t new_node, int new_loc)
    {
        element::node *n = (element::node *)lnode;
        n->update_mutex.lock();
        for (auto &it: n->out_edges)
        {
            element::edge *e = it.second;
            if (e->nbr.handle == orig_node && e->nbr.loc == orig_loc) {
                e->nbr.handle = new_node;
                e->nbr.loc = new_loc;
            }
        }
        n->update_mutex.unlock();
    }

    inline bool
    graph :: check_request(size_t req_id)
    {
        bool ret;
        request_mutex.lock();
        ret = (done_requests.find(req_id) != done_requests.end());
        request_mutex.unlock();
        return ret;
    }

    inline void
    graph :: add_done_request(size_t req_id)
    {
        request_mutex.lock();
        done_requests.insert(req_id);
        request_mutex.unlock();
    }

    inline void
    graph :: broadcast_done_request(size_t req_id)
    {
        int i;
        message::message msg;
        for (i = 0; i < num_shards; i++)
        {
            message::prepare_message(msg, message::REACHABLE_DONE, req_id);
            if (i == myid) {
                continue;
            }
            send(i, msg.buf);
        }
    }

    inline bool
    graph :: mark_visited(element::node *n, size_t req_counter)
    {
        return n->check_and_add_seen(req_counter);
    }

    inline void
    graph :: remove_visited(element::node *n, size_t req_counter)
    {
        n->remove_seen(req_counter);
    }

    inline void 
    graph :: record_visited(size_t coord_req_id, const std::vector<size_t>& nodes)
    {
        visited_mutex.lock();
        if (visit_map) {
            visit_map_even[coord_req_id].insert(visit_map_even[coord_req_id].end(), nodes.begin(), nodes.end());
        } else {
            visit_map_odd[coord_req_id].insert(visit_map_odd[coord_req_id].end(), nodes.begin(), nodes.end());
        }
        visited_mutex.unlock();
    }
    
    inline size_t 
    graph :: get_cache(size_t local_node, size_t dest_loc, size_t dest_node,
        std::vector<common::property>& edge_props)
    {
        return cache.get_req_id(dest_loc, dest_node, local_node, edge_props);
    }

    inline void 
    graph :: add_cache(size_t local_node, size_t dest_loc, size_t dest_node, size_t req_id,
        std::vector<common::property>& edge_props)
    {
        element::node *n = (element::node *)local_node;
        if (cache.insert_entry(dest_loc, dest_node, local_node, req_id, edge_props)) {
            n->add_cached_req(req_id);
        }
    }

    inline void
    graph :: transient_add_cache(size_t local_node, size_t dest_loc, size_t dest_node, size_t req_id,
        std::vector<common::property>& edge_props)
    {
        element::node *n = (element::node *)local_node;
        if (cache.transient_insert_entry(dest_loc, dest_node, local_node, req_id, edge_props)) {
            n->add_cached_req(req_id);
        }
    }

    inline void
    graph :: remove_cache(size_t req_id, element::node *ignore_node = NULL)
    {
        std::unique_ptr<std::vector<size_t>> caching_nodes1 = std::move(cache.remove_entry(req_id));
        std::unique_ptr<std::vector<size_t>> caching_nodes2 = std::move(cache.remove_transient_entry(req_id));
        if (caching_nodes1) {
            for (auto iter: *caching_nodes1)
            {
                element::node *n = (element::node *)(iter);
                if (n != ignore_node) {
                    n->update_mutex.lock();
                    n->remove_cached_req(req_id);
                    n->update_mutex.unlock();
                }
            }
        } 
        if (caching_nodes2) {
            for (auto iter: *caching_nodes2)
            {
                element::node *n = (element::node *)(iter);
                if (n != ignore_node) {
                    n->update_mutex.lock();
                    n->remove_cached_req(req_id);
                    n->update_mutex.unlock();
                }
            }
        }
    }

    inline void
    graph :: commit_cache(size_t req_id)
    {
        cache.commit(req_id);
    }

    inline busybee_returncode
    graph :: send(po6::net::location loc, std::auto_ptr<e::buffer> msg)
    {
        busybee_returncode ret;
        bb_lock.lock();
        if ((ret = bb.send(loc, msg)) != BUSYBEE_SUCCESS)
        {
            std::cerr << "msg send error: " << ret << std::endl;
        }
        bb_lock.unlock();
        return ret;
    }
    
    inline busybee_returncode
    graph :: send(std::unique_ptr<po6::net::location> loc, std::auto_ptr<e::buffer> msg)
    {
        busybee_returncode ret;
        bb_lock.lock();
        if ((ret = bb.send(*loc, msg)) != BUSYBEE_SUCCESS)
        {
            std::cerr << "msg send error: " << ret << std::endl;
        }
        bb_lock.unlock();
        return ret;
    }
    
    inline busybee_returncode
    graph :: send(po6::net::location *loc, std::auto_ptr<e::buffer> msg)
    {
        busybee_returncode ret;
        bb_lock.lock();
        if ((ret = bb.send(*loc, msg)) != BUSYBEE_SUCCESS)
        {
            std::cerr << "msg send error: " << ret << std::endl;
        }
        bb_lock.unlock();
        return ret;
    }

    inline busybee_returncode
    graph :: send(int loc, std::auto_ptr<e::buffer> msg)
    {
        if (loc == -1) {
            return send_coord(msg);
        }
        busybee_returncode ret;
        bb_lock.lock();
        if ((ret = bb.send(*shard_locs[loc], msg)) != BUSYBEE_SUCCESS)
        {
            std::cerr << "msg send error: " << ret << std::endl;
        }
        bb_lock.unlock();
        return ret;
    }

    inline busybee_returncode
    graph :: send_coord(std::auto_ptr<e::buffer> msg)
    {
        busybee_returncode ret;
        bb_lock.lock();
        if ((ret = bb.send(*coord, msg)) != BUSYBEE_SUCCESS)
        {
            std::cerr << "msg send error: " << ret << std::endl;
        }
        bb_lock.unlock();
        return ret;
    }

    // caution: assuming we hold the request->mutex
    inline void 
    graph :: propagate_request(std::vector<size_t> &nodes, std::shared_ptr<batch_request> request, int prop_loc)
    {
        message::message msg(message::REACHABLE_PROP);
        size_t my_outgoing_req_id;
        outgoing_req_id_counter_mutex.lock();
        my_outgoing_req_id = outgoing_req_id_counter++;
        // XXX what the, I don't even..
        std::pair<size_t, std::shared_ptr<batch_request>> new_elem(my_outgoing_req_id, request);
        assert(pending_batch.insert(new_elem).second);
        outgoing_req_id_counter_mutex.unlock();
        message::prepare_message(msg, message::REACHABLE_PROP, request->vector_clock, nodes, myid,
            request->dest_addr, request->dest_loc, request->coord_id, my_outgoing_req_id, 
            request->edge_props, request->ignore_cache);
        // no local messages possible, so have to send via network
        send(prop_loc, msg.buf);
    }

    inline bool
    graph :: migrate_test()
    {
        update_mutex.lock();
        if (!already_migrated) {
            already_migrated = true;
            update_mutex.unlock();
            return true;
        } else {
            update_mutex.unlock();
            return false;
        }
    }

    void
    graph :: sort_and_print_nodes()
    {
        std::sort(nodes.begin(), nodes.end(), element::compare_msg_cnt);
        for (auto &n: nodes)
        {
            std::cout << "Node " << (void*)n;
        }
    }

    inline bool
    thread :: unstarted_traversal_thread :: operator>(const unstarted_traversal_thread &t) const
    {
        return (*req > *t.req); 
    }

    inline bool
    thread :: unstarted_update_thread :: operator>(const unstarted_update_thread &t) const
    {
        return (*req > *t.req); 
    }

    void
    thread :: traversal_thread_loop(thread::pool *tpool)
    {
        thread::unstarted_traversal_thread *thr;
        std::priority_queue<thread::unstarted_traversal_thread*, 
            std::vector<unstarted_traversal_thread*>, 
            thread::traversal_req_compare> &tq = tpool->traversal_queue;
        po6::threads::mutex &m = tpool->queue_mutex;
        po6::threads::cond &c = tpool->traversal_queue_cond;
        bool can_start;
        while (true)
        {
            m.lock();
            if (!tq.empty()) {
                thr = tq.top();
                can_start = thr->G->check_clock(thr->req->start_time);
            }
            while (tq.empty() || !can_start)
            {
                c.wait();
                if (!tq.empty()) {
                    thr = tq.top();
                    can_start = thr->G->check_clock(thr->req->start_time);
                }
            }
            tq.pop();
            c.broadcast();
            m.unlock();
            (*thr->func)(thr->G, thr->req);
            delete thr;
        }
    }

    void
    thread :: update_thread_loop(thread::pool *tpool)
    {
        thread::unstarted_update_thread *thr;
        std::priority_queue<thread::unstarted_update_thread*, 
            std::vector<unstarted_update_thread*>, 
            thread::update_req_compare> &tq = tpool->update_queue;
        po6::threads::mutex &m = tpool->queue_mutex;
        po6::threads::cond &c = tpool->update_queue_cond;
        bool can_start;
        while (true)
        {
            m.lock();
            if (!tq.empty()) {
                thr = tq.top();
                can_start = thr->G->check_clock(thr->req->start_time);
            }
            while (tq.empty() || !can_start)
            {
                c.wait();
                if (!tq.empty()) {
                    thr = tq.top();
                    can_start = thr->G->check_clock(thr->req->start_time);
                }
            }
            tq.pop();
            c.broadcast();
            m.unlock();
            (*thr->func)(thr->G, thr->req);
            delete thr;
        }
    }

} //namespace db

#endif //__GRAPH__
