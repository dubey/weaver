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
#include "request_objects.h"

namespace db
{
    class perm_del
    {
        public:
            enum message::msg_type type;
            uint64_t req_id;
            union 
            {
                element::node *del_node;
                struct 
                {
                    element::node *n;
                    size_t edge_handle;
                } del_edge;
                struct 
                {
                    element::node *n;
                    size_t edge_handle;
                    uint32_t key;
                } del_edge_prop;
            } request;

        public:
            inline
            perm_del(enum message::msg_type t, uint64_t id, element::node *dn)
                : type(t)
                , req_id(id)
            {
                request.del_node = dn;
            }

            inline
            perm_del(enum message::msg_type t, uint64_t id, element::node *dn, size_t edge)
                : type(t)
                , req_id(id)
            {
                request.del_edge.n = dn;
                request.del_edge.edge_handle = edge;
            }
            
            inline
            perm_del(enum message::msg_type t, uint64_t id, element::node *dn, size_t edge, uint32_t k)
                : type(t)
                , req_id(id)
            {
                request.del_edge_prop.n = dn;
                request.del_edge_prop.edge_handle = edge;
                request.del_edge_prop.key = k;
            }
    };

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

    // migration state
    class migrate_request
    {
        public:
            po6::threads::mutex mutex;
            element::node *cur_node; // node being migrated
            int new_loc; // shard to which this node is being migrated
            size_t migr_node; // node handle on new shard
            std::vector<std::unique_ptr<message::message>> pending_updates; // queued updates
            std::vector<uint64_t> pending_update_ids; // ids of queued updates
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
            uint64_t outgoing_req_id_counter;
            po6::threads::mutex outgoing_req_id_counter_mutex, visited_mutex;
            std::unordered_map<uint64_t, std::shared_ptr<batch_request>> pending_batch;
            std::unordered_set<uint64_t> done_requests; // requests which need to be killed // TODO need clean up of old done requests
            db::thread::pool thread_pool;
            std::unordered_map<uint64_t, std::vector<size_t>> visit_map_odd, visit_map_even; // visited ids -> nodes
            bool visit_map;
            // For node migration
            migrate_request mrequest; // pending migration request object
            po6::threads::mutex migration_mutex;
            element::node *migr_node; // newly migrated node
            std::priority_queue<update_request*, std::vector<update_request*>, req_compare> pending_updates;
            std::deque<uint64_t> pending_update_ids;
            uint64_t target_clock;
            // For permanent deletion
            std::vector<perm_del> delete_req_ids;
            // testing
            std::unordered_map<uint64_t, uint64_t> req_count;
            bool already_migrated;
            
        public:
            // consistency
            bool check_clock(uint64_t time);
            bool check_request(uint64_t req_id);
            bool increment_clock();
            // graph update
            element::node* create_node(uint64_t time, bool migrate);
            std::pair<bool, std::unique_ptr<std::vector<uint64_t>>> delete_node(element::node *n, uint64_t del_time);
            std::pair<bool, size_t> create_edge(size_t local_node, uint64_t time, size_t remote_node, int remote_loc, uint64_t remote_time);
            bool create_reverse_edge(uint64_t time, size_t local_node, size_t remote_node, int remote_loc);
            std::pair<bool, std::unique_ptr<std::vector<uint64_t>>> delete_edge(element::node *n, size_t edge_handle, uint64_t del_time);
            bool add_edge_property(size_t n, size_t e, common::property &prop);
            std::pair<bool, std::unique_ptr<std::vector<uint64_t>>> delete_all_edge_property(size_t n, size_t e, uint32_t key, uint64_t del_time);
            bool permanent_delete(uint64_t req_id);
            // traversals
            void add_done_request(uint64_t req_id);
            void broadcast_done_request(uint64_t req_id); 
            bool mark_visited(element::node *n, uint64_t req_id);
            void remove_visited(element::node *n, uint64_t req_id);
            void record_visited(uint64_t coord_req_id, const std::vector<uint64_t> &nodes);
            size_t get_cache(size_t local_node, size_t dest_loc, size_t dest_node, std::vector<common::property> &edge_props);
            void add_cache(size_t local_node, size_t dest_loc, size_t dest_node, uint64_t req_id, std::vector<common::property> &edge_props);
            void transient_add_cache(size_t local_node, size_t dest_loc, size_t dest_node, uint64_t req_id, std::vector<common::property> &edge_props);
            void remove_cache(uint64_t req_id, element::node *);
            void commit_cache(uint64_t req_id);
            void propagate_request(std::vector<size_t> &nodes, std::shared_ptr<batch_request> request, int prop_loc);
            // migration
            void update_migrated_nbr(size_t lnode, size_t orig_node, int orig_loc, size_t new_node, int new_loc);
            bool set_target_clock(uint64_t time);
            void set_update_ids(std::vector<uint64_t>&);
            void queue_transit_node_update(uint64_t req_id, std::unique_ptr<message::message> msg);
            // busybee send methods
            busybee_returncode send(po6::net::location loc, std::auto_ptr<e::buffer> buf);
            busybee_returncode send(std::unique_ptr<po6::net::location> loc, std::auto_ptr<e::buffer> buf);
            busybee_returncode send(po6::net::location *loc, std::auto_ptr<e::buffer> buf);
            busybee_returncode send(int loc, std::auto_ptr<e::buffer> buf);
            busybee_returncode send_coord(std::auto_ptr<e::buffer> buf);
            // XXX
            void refresh_edge(element::node *n, element::edge *e, uint64_t del_time);
            // testing
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
        while (file >> ipaddr >> inport) {
            temp_loc = new po6::net::location(ipaddr.c_str(), inport);
            shard_locs[i] = temp_loc;
            ++i;
        }
        file.close();
    }

    // consistency methods

    inline bool
    graph :: check_clock(uint64_t time)
    {
        bool ret;
        update_mutex.lock();
        ret = (time <= my_clock);
        update_mutex.unlock();
        return ret;
    }

    inline bool
    graph :: check_request(uint64_t req_id)
    {
        bool ret;
        request_mutex.lock();
        ret = (done_requests.find(req_id) != done_requests.end());
        request_mutex.unlock();
        return ret;
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
        thread_pool.work_queue_cond.broadcast();
        thread_pool.queue_mutex.unlock();
        return check;
    }

    // graph update methods

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

    inline std::pair<bool, std::unique_ptr<std::vector<uint64_t>>>
    graph :: delete_node(element::node *n, uint64_t del_time)
    {
        std::pair<bool, std::unique_ptr<std::vector<uint64_t>>> ret;
        n->update_mutex.lock();
        if (n->state == element::node::mode::IN_TRANSIT) {
            ret.first = false;
        } else {
            n->update_del_time(del_time);
            ret.first = true;
        }
        ret.second = std::move(n->purge_cache());
        for (auto rid: *ret.second) {
            remove_cache(rid, n);
        }
        n->update_mutex.unlock();
        update_mutex.lock();
        delete_req_ids.emplace_back(message::NODE_DELETE_REQ, del_time, n);
        update_mutex.unlock();
        return ret;
    }

    inline std::pair<bool, size_t>
    graph :: create_edge(size_t local_node, uint64_t time, size_t remote_node, int remote_loc, uint64_t remote_time)
    {
        std::pair<bool, size_t> ret;
        element::node *n = (element::node *)local_node;
        n->update_mutex.lock();
        if (n->state == element::node::mode::IN_TRANSIT) {
            n->update_mutex.unlock();
            ret.first = false;
        } else {
            element::edge *new_edge = new element::edge(time, remote_loc, remote_node);
            ret.second = n->add_edge(new_edge, true);
            n->update_mutex.unlock();
            message::message msg(message::REVERSE_EDGE_CREATE);
            message::prepare_message(msg, message::REVERSE_EDGE_CREATE, remote_time, time, local_node, myid, remote_node);
            send(remote_loc, msg.buf);
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
            std::cout << "New rev edge: " << (void*)new_edge->nbr.handle << " " 
                      << new_edge->nbr.loc << " at lnode " << (void*)local_node << std::endl;
            std::cout << " in edge size " << n->in_edges.size() << std::endl;
#endif
            ret = true;
        }
        n->update_mutex.unlock();
        return ret;
    }

    inline std::pair<bool, std::unique_ptr<std::vector<uint64_t>>>
    graph :: delete_edge(element::node *n, size_t edge_handle, uint64_t del_time)
    {
        std::pair<bool, std::unique_ptr<std::vector<uint64_t>>> ret;
        n->update_mutex.lock();
        if (n->state == element::node::mode::IN_TRANSIT) {
            ret.first = false;
        } else {
            element::edge *e = n->out_edges.at(edge_handle);
            e->update_del_time(del_time);
            ret.first = true;
        }
        ret.second = std::move(n->purge_cache());
        for (auto rid: *ret.second) {
            remove_cache(rid, n);
        }
        n->update_mutex.unlock();
        update_mutex.lock();
        delete_req_ids.emplace_back(message::NODE_DELETE_REQ, del_time, n, edge_handle);
        update_mutex.unlock();
        return ret;
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

    inline std::pair<bool, std::unique_ptr<std::vector<uint64_t>>>
    graph :: delete_all_edge_property(size_t node, size_t edge, uint32_t key, uint64_t time)
    {
        element::node *n = (element::node*)node;
        element::edge *e = n->out_edges.at(edge);
        std::pair<bool, std::unique_ptr<std::vector<uint64_t>>> ret;
        n->update_mutex.lock();
        if (n->state == element::node::mode::IN_TRANSIT) {
            ret.first = false;
        } else {
            e->delete_property(key, time);
            ret.first = true;
        }
        ret.second = std::move(n->purge_cache());
        for (auto rid: *ret.second) {
            remove_cache(rid, n);
        }
        n->update_mutex.unlock();
        update_mutex.lock();
        delete_req_ids.emplace_back(message::NODE_DELETE_REQ, time, n, edge, key);
        update_mutex.unlock();
        return ret;
    }

    bool 
    graph :: permanent_delete(uint64_t req_id)
    {
        return false; // TODO
    }

    // traversal methods

    inline void
    graph :: add_done_request(uint64_t req_id)
    {
        request_mutex.lock();
        done_requests.insert(req_id);
        request_mutex.unlock();
    }

    inline void
    graph :: broadcast_done_request(uint64_t req_id)
    {
        int i;
        message::message msg;
        for (i = 0; i < num_shards; i++) {
            message::prepare_message(msg, message::REACHABLE_DONE, req_id);
            if (i == myid) {
                continue;
            }
            send(i, msg.buf);
        }
    }

    inline bool
    graph :: mark_visited(element::node *n, uint64_t req_id)
    {
        return n->check_and_add_seen(req_id);
    }

    inline void
    graph :: remove_visited(element::node *n, uint64_t req_id)
    {
        n->remove_seen(req_id);
    }

    inline void 
    graph :: record_visited(uint64_t coord_req_id, const std::vector<size_t> &nodes)
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
    graph :: get_cache(size_t local_node, size_t dest_loc, size_t dest_node, std::vector<common::property> &edge_props)
    {
        return cache.get_req_id(dest_loc, dest_node, local_node, edge_props);
    }

    inline void 
    graph :: add_cache(size_t local_node, size_t dest_loc, size_t dest_node, uint64_t req_id, std::vector<common::property> &edge_props)
    {
        element::node *n = (element::node *)local_node;
        if (cache.insert_entry(dest_loc, dest_node, local_node, req_id, edge_props)) {
            n->add_cached_req(req_id);
        }
    }

    inline void
    graph :: transient_add_cache(size_t local_node, size_t dest_loc, size_t dest_node, uint64_t req_id, std::vector<common::property> &edge_props)
    {
        element::node *n = (element::node *)local_node;
        if (cache.transient_insert_entry(dest_loc, dest_node, local_node, req_id, edge_props)) {
            n->add_cached_req(req_id);
        }
    }

    inline void
    graph :: remove_cache(uint64_t req_id, element::node *ignore_node = NULL)
    {
        std::unique_ptr<std::vector<size_t>> caching_nodes1 = std::move(cache.remove_entry(req_id));
        std::unique_ptr<std::vector<size_t>> caching_nodes2 = std::move(cache.remove_transient_entry(req_id));
        if (caching_nodes1) {
            for (auto iter: *caching_nodes1) {
                element::node *n = (element::node *)(iter);
                if (n != ignore_node) {
                    n->update_mutex.lock();
                    n->remove_cached_req(req_id);
                    n->update_mutex.unlock();
                }
            }
        } 
        if (caching_nodes2) {
            for (auto iter: *caching_nodes2) {
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
    graph :: commit_cache(uint64_t req_id)
    {
        cache.commit(req_id);
    }

    // caution: assuming we hold the request->mutex
    inline void 
    graph :: propagate_request(std::vector<size_t> &nodes, std::shared_ptr<batch_request> request, int prop_loc)
    {
        message::message msg(message::REACHABLE_PROP);
        uint64_t my_outgoing_req_id;
        outgoing_req_id_counter_mutex.lock();
        my_outgoing_req_id = outgoing_req_id_counter++;
        std::pair<size_t, std::shared_ptr<batch_request>> new_elem(my_outgoing_req_id, request);
        assert(pending_batch.insert(new_elem).second);
        outgoing_req_id_counter_mutex.unlock();
        message::prepare_message(msg, message::REACHABLE_PROP, request->vector_clock, nodes, myid,
            request->dest_addr, request->dest_loc, request->coord_id, my_outgoing_req_id, 
            request->edge_props, request->ignore_cache);
        // no local messages possible, so have to send via network
        send(prop_loc, msg.buf);
    }


    // migration methods

    inline void
    graph :: update_migrated_nbr(size_t local_node, size_t orig_node, int orig_loc, size_t new_node, int new_loc)
    {
        element::node *n = (element::node *)local_node;
        n->update_mutex.lock();
        for (auto &it: n->out_edges) {
            element::edge *e = it.second;
            if (e->nbr.handle == orig_node && e->nbr.loc == orig_loc) {
                e->nbr.handle = new_node;
                e->nbr.loc = new_loc;
            }
        }
        n->update_mutex.unlock();
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
    graph :: set_update_ids(std::vector<uint64_t> &update_ids)
    {
        migration_mutex.lock();
        for (uint64_t id: update_ids) {
            pending_update_ids.push_back(id);
        }
        migration_mutex.unlock();
    }

    inline void
    graph :: queue_transit_node_update(uint64_t req_id, std::unique_ptr<message::message> msg)
    {
        mrequest.mutex.lock();
        mrequest.pending_updates.emplace_back(std::move(msg));
        mrequest.pending_update_ids.emplace_back(req_id);
        mrequest.mutex.unlock();
    }

    // busybee send methods
   
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

    //XXX where is this used?
    inline void
    graph :: refresh_edge(element::node *n, element::edge *e, uint64_t del_time)
    {
        // TODO caching and edge refreshes
        n->update_mutex.lock();
        e->update_del_time(del_time);
        n->update_mutex.unlock();
    }

    // testing methods
    void
    graph :: sort_and_print_nodes()
    {
        std::sort(nodes.begin(), nodes.end(), element::compare_msg_cnt);
        for (auto &n: nodes) {
            std::cout << "Node " << (void*)n;
        }
    }

    // work loop for threads in thread pool
    // pull request off job priority queue, check if it is okay to run
    // if yes, execute, else sleep and wait for clock to be incremented
    void
    thread :: worker_thread_loop(thread::pool *tpool)
    {
        thread::unstarted_thread *thr;
        std::priority_queue<thread::unstarted_thread*, 
            std::vector<unstarted_thread*>, 
            thread::work_thread_compare> &tq = tpool->work_queue;
        po6::threads::mutex &m = tpool->queue_mutex;
        po6::threads::cond &c = tpool->work_queue_cond;
        bool can_start;
        while (true) {
            m.lock();
            if (!tq.empty()) {
                thr = tq.top();
                can_start = thr->G->check_clock(thr->start_time); // as priority is equal to the start time of the req
            }
            while (tq.empty() || !can_start) {
                c.wait();
                if (!tq.empty()) {
                    thr = tq.top();
                    can_start = thr->G->check_clock(thr->start_time);
                }
            }
            tq.pop();
            c.broadcast();
            m.unlock();
            (*thr->func)(thr->G, thr->arg);
            delete thr;
        }
    }

} //namespace db

#endif //__GRAPH__
