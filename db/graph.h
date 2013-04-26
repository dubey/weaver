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
#include <deque>
#include <fstream>
#include <iostream>
#include <unordered_map>
#include <po6/net/location.h>
#include <po6/threads/mutex.h>
#include <po6/threads/cond.h>
#include <busybee_sta.h>

#include "common/weaver_constants.h"
#include "common/property.h"
#include "element/node.h"
#include "element/edge.h"
#include "threadpool/threadpool.h"
#include "node_prog/node_prog_type.h"
#include "cache/program_cache.h"
#include "state/program_state.h"

namespace db
{
    class perm_del
    {
        public:
            enum message::msg_type type;
            uint64_t req_id;
            union {
                struct {
                    uint64_t node_handle;
                } del_node;
                struct {
                    uint64_t node_handle;
                    uint64_t edge_handle;
                } del_edge;
                struct {
                    uint64_t node_handle;
                    uint64_t edge_handle;
                    uint32_t key;
                } del_edge_prop;
            } request;

        public:
            inline
            perm_del(enum message::msg_type t, uint64_t id, uint64_t node)
                : type(t)
                , req_id(id)
            {
                request.del_node.node_handle = node;
            }

            inline
            perm_del(enum message::msg_type t, uint64_t id, uint64_t node, uint64_t edge)
                : type(t)
                , req_id(id)
            {
                request.del_edge.node_handle = node;
                request.del_edge.edge_handle = edge;
            }
            
            inline
            perm_del(enum message::msg_type t, uint64_t id, uint64_t node, uint64_t edge, uint32_t k)
                : type(t)
                , req_id(id)
            {
                request.del_edge_prop.node_handle = node;
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
            uint64_t cur_node; // node being migrated
            //element::node *cur_node;
            int new_loc; // shard to which this node is being migrated
            uint64_t migr_node; // node handle on new shard
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
            po6::threads::mutex clock_mutex, update_mutex, request_mutex, deletion_mutex;
            uint64_t my_clock;

        public:
            // Consistency
            bool check_clock(uint64_t time);
            bool increment_clock();
            element::node* acquire_node(uint64_t node_handle);
            void release_node(element::node *n);

            // Graph state
            std::unordered_map<uint64_t, element::node*> nodes;
            std::shared_ptr<po6::net::location> myloc;
            std::shared_ptr<po6::net::location> coord;
            po6::net::location **shard_locs; // po6 locations for each shard
            int myid;
            db::thread::pool thread_pool;
            void create_node(uint64_t time, bool migrate);
            std::pair<bool, std::unique_ptr<std::vector<uint64_t>>> delete_node(uint64_t node, uint64_t del_time);
            bool create_edge(uint64_t local_node, uint64_t time, uint64_t remote_node, int remote_loc, uint64_t remote_time);
            bool create_reverse_edge(uint64_t time, uint64_t local_node, uint64_t remote_node, int remote_loc);
            std::pair<bool, std::unique_ptr<std::vector<uint64_t>>> delete_edge(uint64_t node, uint64_t edge_handle, uint64_t del_time);
            bool add_edge_property(uint64_t n, uint64_t e, common::property &prop);
            std::pair<bool, std::unique_ptr<std::vector<uint64_t>>> delete_all_edge_property(uint64_t n, uint64_t e, uint32_t key, uint64_t del_time);
            void permanent_delete(uint64_t req_id);
            void permanent_edge_delete(uint64_t n, uint64_t e);

            // Node migration
            migrate_request mrequest; // pending migration request object
            po6::threads::mutex migration_mutex;
            uint64_t migr_node; // newly migrated node
            std::priority_queue<update_request*, std::vector<update_request*>, req_compare> pending_updates;
            std::deque<uint64_t> pending_update_ids;
            uint64_t target_clock;
            void update_migrated_nbr(uint64_t lnode, uint64_t orig_node, int orig_loc, uint64_t new_node, int new_loc);
            bool set_target_clock(uint64_t time);
            void set_update_ids(std::vector<uint64_t>&);
            void queue_transit_node_update(uint64_t req_id, std::unique_ptr<message::message> msg);
            
            // Permanent deletion
            uint64_t del_id;
            std::deque<perm_del> delete_req_ids;
            
            // Messaging infrastructure
            busybee_sta bb; // Busybee instance used for sending messages
            busybee_sta bb_recv; // Busybee instance used for receiving messages
            po6::threads::mutex bb_lock; // Busybee lock
            busybee_returncode send(po6::net::location loc, std::auto_ptr<e::buffer> buf);
            busybee_returncode send(std::unique_ptr<po6::net::location> loc, std::auto_ptr<e::buffer> buf);
            busybee_returncode send(po6::net::location *loc, std::auto_ptr<e::buffer> buf);
            busybee_returncode send(int loc, std::auto_ptr<e::buffer> buf);
            busybee_returncode send_coord(std::auto_ptr<e::buffer> buf);
            
            // testing
            std::unordered_map<uint64_t, uint64_t> req_count;
            void sort_and_print_nodes();

            // Node programs
            // prog_type-> map from request_id to map from node handle to request state for that node
            state::program_state node_prog_req_state; 
            bool prog_req_state_exists(node_prog::prog_type t, uint64_t request_id, uint64_t local_node_handle);
            node_prog::Deletable* fetch_prog_req_state(node_prog::prog_type t, uint64_t request_id, uint64_t local_node_handle);
            void insert_prog_req_state(node_prog::prog_type t, uint64_t request_id, uint64_t local_node_handle, node_prog::Deletable *toAdd);
            // prog_type-> map from node handle to map from request_id to cache values -- used to do cache read/updates
            cache::program_cache node_prog_cache;
            bool prog_cache_exists(node_prog::prog_type t, uint64_t local_node_handle, uint64_t req_id);
            std::vector<node_prog::CacheValueBase*> fetch_prog_cache(node_prog::prog_type t, uint64_t local_node_handle, uint64_t req_id, std::vector<uint64_t> *dirty_list_ptr, std::unordered_set<uint64_t> &ignore_set);
            node_prog::CacheValueBase* fetch_prog_cache_single(node_prog::prog_type t, uint64_t local_node_handle, uint64_t req_id);
            void insert_prog_cache(node_prog::prog_type t, uint64_t request_id, uint64_t local_node_handle, node_prog::CacheValueBase *toAdd, element::node *n);
            void invalidate_prog_cache(uint64_t request_id);
            void commit_prog_cache(uint64_t req_id);
    };

    inline
    graph :: graph(int my_id, const char* ip_addr, int port)
        : my_clock(0)
        , myloc(new po6::net::location(ip_addr, port))
        , coord(new po6::net::location(COORD_IPADDR, COORD_REC_PORT))
        , myid(my_id)
        , thread_pool(NUM_THREADS)
        , target_clock(0)
        , bb(myloc->address, myloc->port + SEND_PORT_INCR, 0)
        , bb_recv(myloc->address, myloc->port, 0)
    {
        int i, inport;
        po6::net::location *temp_loc;
        std::string ipaddr;
        std::ifstream file(SHARDS_DESC_FILE);
        shard_locs = (po6::net::location **)malloc(sizeof(po6::net::location *) * NUM_SHARDS);
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
        clock_mutex.lock();
        ret = (time <= my_clock);
        clock_mutex.unlock();
        return ret;
    }

    inline bool
    graph :: increment_clock()
    {
        bool check;
        clock_mutex.lock();
        my_clock++;
        check = (my_clock == target_clock);
        if (check) {
            target_clock = 0;
        }
        clock_mutex.unlock();
        thread_pool.queue_mutex.lock();
        thread_pool.work_queue_cond.broadcast();
        thread_pool.queue_mutex.unlock();
        return check;
    }

    // graph update methods

    // find the node corresponding to given handle
    // lock and return the node
    inline element::node*
    graph :: acquire_node(uint64_t node_handle)
    {
        element::node *n = NULL;
        update_mutex.lock();
        if (nodes.find(node_handle) != nodes.end()) {
            n = nodes.at(node_handle);
            n->update_mutex.lock();
        }
        update_mutex.unlock();
        return n;
    }

    inline void
    graph :: release_node(element::node *n)
    {
        n->update_mutex.unlock();
    }

    inline void
    graph :: create_node(uint64_t time, bool migrate = false)
    {
        element::node *new_node = new element::node(time);
        update_mutex.lock();
        nodes.emplace(time, new_node);
        update_mutex.unlock();
        if (migrate) {
            migration_mutex.lock();
            migr_node = time;
            migration_mutex.unlock();
        } else {
            new_node->state = element::node::mode::STABLE;
        }
#ifdef DEBUG
        std::cout << "Creating node, addr = " << (void*) new_node << std::endl;
#endif
    }

    inline std::pair<bool, std::unique_ptr<std::vector<uint64_t>>>
    graph :: delete_node(uint64_t node_handle, uint64_t del_time)
    {
        std::pair<bool, std::unique_ptr<std::vector<uint64_t>>> ret;
        element::node *n = acquire_node(node_handle);
        if (n->state == element::node::mode::IN_TRANSIT) {
            ret.first = false;
        } else {
            n->update_del_time(del_time);
            ret.first = true;
        }
        ret.second = std::move(n->purge_cache());
        for (auto rid: *ret.second) {
            //remove_cache(rid, node_handle);
        }
        release_node(n);
        deletion_mutex.lock();
        delete_req_ids.emplace_back(message::NODE_DELETE_REQ, del_time, node_handle);
        deletion_mutex.unlock();
        return ret;
    }

    inline bool
    graph :: create_edge(uint64_t local_node, uint64_t time, uint64_t remote_node, int remote_loc, uint64_t remote_time)
    {
        bool ret;
        element::node *n = acquire_node(local_node);
        if (n->state == element::node::mode::IN_TRANSIT) {
            release_node(n);
            ret = false;
        } else {
            element::edge *new_edge = new element::edge(time, remote_loc, remote_node);
            n->add_edge(new_edge, true);
            release_node(n);
            message::message msg(message::REVERSE_EDGE_CREATE);
            message::prepare_message(msg, message::REVERSE_EDGE_CREATE, remote_time, time, local_node, myid, remote_node);
            send(remote_loc, msg.buf);
#ifdef DEBUG
            std::cout << "Creating edge, addr = " << (void*) new_edge << std::endl;
#endif
            ret = true;
        }
        return ret;
    }

    inline bool
    graph :: create_reverse_edge(uint64_t time, uint64_t local_node, uint64_t remote_node, int remote_loc)
    {
        bool ret;
        element::node *n = acquire_node(local_node);
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
        release_node(n);
        return ret;
    }

    inline std::pair<bool, std::unique_ptr<std::vector<uint64_t>>>
    graph :: delete_edge(uint64_t node_handle, uint64_t edge_handle, uint64_t del_time)
    {
        std::pair<bool, std::unique_ptr<std::vector<uint64_t>>> ret;
        element::node *n = acquire_node(node_handle);
        if (n->state == element::node::mode::IN_TRANSIT) {
            ret.first = false;
        } else {
            element::edge *e = n->out_edges.at(edge_handle);
            e->update_del_time(del_time);
            ret.first = true;
        }
        ret.second = std::move(n->purge_cache());
        for (auto rid: *ret.second) {
            //remove_cache(rid, node_handle);
        }
        release_node(n);
        deletion_mutex.lock();
        delete_req_ids.emplace_back(message::EDGE_DELETE_REQ, del_time, node_handle, edge_handle);
        deletion_mutex.unlock();
        return ret;
    }

    inline bool
    graph :: add_edge_property(uint64_t node, uint64_t edge, common::property &prop)
    {
        element::node *n = acquire_node(node);
        element::edge *e = n->out_edges.at(edge);
        bool ret;
        if (n->state == element::node::mode::IN_TRANSIT) {
            ret = false;
        } else {
            e->add_property(prop);
            ret = true;
        }
        release_node(n);
        return ret;
    }

    inline std::pair<bool, std::unique_ptr<std::vector<uint64_t>>>
    graph :: delete_all_edge_property(uint64_t node, uint64_t edge, uint32_t key, uint64_t time)
    {
        element::node *n = acquire_node(node);
        element::edge *e = n->out_edges.at(edge);
        std::pair<bool, std::unique_ptr<std::vector<uint64_t>>> ret;
        if (n->state == element::node::mode::IN_TRANSIT) {
            ret.first = false;
        } else {
            e->delete_property(key, time);
            ret.first = true;
        }
        ret.second = std::move(n->purge_cache());
        for (auto rid: *ret.second) {
            //remove_cache(rid, node);
        }
        release_node(n);
        deletion_mutex.lock();
        delete_req_ids.emplace_back(message::EDGE_DELETE_PROP, time, node, edge, key);
        deletion_mutex.unlock();
        return ret;
    }

    inline void
    graph :: permanent_delete(uint64_t req_id)
    {
        deletion_mutex.lock();
        del_id = req_id;
        while (!delete_req_ids.empty()) {
            perm_del &front = delete_req_ids.front();
            if (front.req_id <= req_id) {
                // can delete top element
                switch (front.type) {
                    case message::NODE_DELETE_REQ: {
                        message::message msg;
                        auto &req = front.request.del_node;
                        element::node *n;
                        element::edge *e;
                        update_mutex.lock();
                        n = nodes.at(req.node_handle);
                        nodes.erase(req.node_handle);
                        n->update_mutex.lock();
                        update_mutex.unlock();
                        for (auto &it: n->out_edges) {
                            e = it.second;
                            message::prepare_message(msg, message::PERMANENT_DELETE_EDGE, req_id, e->nbr.handle, it.first);
                            send(e->nbr.loc, msg.buf);
                            delete e;
                        }
                        for (auto &it: n->in_edges) {
                            e = it.second;
                            message::prepare_message(msg, message::PERMANENT_DELETE_EDGE, req_id, e->nbr.handle, it.first);
                            send(e->nbr.loc, msg.buf);
                            delete e;
                        }
                        delete n;
                        break;
                    }
                    case message::EDGE_DELETE_REQ: {
                        message::message msg;
                        auto &req = front.request.del_edge;
                        element::node *n = acquire_node(req.node_handle);
                        if (n->out_edges.find(req.edge_handle) != n->out_edges.end()) {
                            element::edge *e = n->out_edges.at(req.edge_handle);
                            message::prepare_message(msg, message::PERMANENT_DELETE_EDGE, req_id, e->nbr.handle, req.edge_handle);
                            send(e->nbr.loc, msg.buf);
                            n->out_edges.erase(req.edge_handle);
                            delete e;
                        } else {
                            std::cout << "Not found edge in perm_del\n";
                        }
                        release_node(n);
                        break;
                    }
                    case message::EDGE_DELETE_PROP: {
                        auto &req = front.request.del_edge_prop;
                        element::node *n = acquire_node(req.node_handle);
                        if (n->out_edges.find(req.edge_handle) != n->out_edges.end()) {
                            element::edge *e = n->out_edges.at(req.edge_handle);
                            e->remove_property(req.key, front.req_id);
                        }
                        release_node(n);
                        break;
                    }
                    default:
                        std::cerr << "Bad type in permanent delete list " << front.type << std::endl;
                }
                delete_req_ids.pop_front();
            } else {
                break;
            }
        }
        deletion_mutex.unlock();
        message::message msg;
        message::prepare_message(msg, message::CACHE_UPDATE_ACK); 
        send_coord(msg.buf);
    }

    inline void
    graph :: permanent_edge_delete(uint64_t node_handle, uint64_t edge_handle)
    {
        element::node *n = NULL;
        element::edge *e;
        message::message msg;
        deletion_mutex.lock();
        n = acquire_node(node_handle);
        if (n == NULL) {
            std::cout << "Not found node in perm_edge_del\n";
            return;
        }
        if (n->in_edges.find(edge_handle) != n->in_edges.end()) {
            e = n->in_edges.at(edge_handle);
            n->in_edges.erase(edge_handle);
            delete e;
        } else if (n->out_edges.find(edge_handle) != n->in_edges.end()) {
            e = n->out_edges.at(edge_handle);
            n->out_edges.erase(edge_handle);
            delete e;
        }
        release_node(n);
        deletion_mutex.unlock();
    }

    // migration methods

    inline void
    graph :: update_migrated_nbr(uint64_t local_node, uint64_t orig_node, int orig_loc, uint64_t new_node, int new_loc)
    {
        element::node *n = acquire_node(local_node);
        for (auto &it: n->out_edges) {
            element::edge *e = it.second;
            if (e->nbr.handle == orig_node && e->nbr.loc == orig_loc) {
                e->nbr.handle = new_node;
                e->nbr.loc = new_loc;
            }
        }
        release_node(n);
    }

    inline bool
    graph :: set_target_clock(uint64_t time)
    {
        bool ret;
        clock_mutex.lock();
        if (time == my_clock) {
            ret = true;
        } else {
            target_clock = time;
            ret = false;
        }
        clock_mutex.unlock();
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

    // testing methods
    void
    graph :: sort_and_print_nodes()
    {
        /*
        std::sort(nodes.begin(), nodes.end(), element::compare_msg_cnt);
        for (auto &it: nodes) {
            void *n = (void*)it.second;
            std::cout << "Node " << n;
        }
        */
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

    // node program
    inline bool 
    graph :: prog_req_state_exists(node_prog::prog_type t, uint64_t request_id, uint64_t local_node_handle)
    {
        return node_prog_req_state.state_exists(t, request_id, local_node_handle);
    }

    inline node_prog::Deletable*
    graph :: fetch_prog_req_state(node_prog::prog_type t, uint64_t request_id, uint64_t local_node_handle)
    {
        return node_prog_req_state.get_state(t, request_id, local_node_handle);
    }

    inline void 
    graph :: insert_prog_req_state(node_prog::prog_type t, uint64_t request_id, uint64_t local_node_handle, node_prog::Deletable* toAdd)
    {
        node_prog_req_state.put_state(t, request_id, local_node_handle, toAdd);
    }

    inline bool
    graph :: prog_cache_exists(node_prog::prog_type t, uint64_t node_handle, uint64_t req_id)
    {
        return node_prog_cache.cache_exists(t, node_handle, req_id);
    }

    inline std::vector<node_prog::CacheValueBase*>
    graph :: fetch_prog_cache(node_prog::prog_type t, uint64_t local_node_handle, uint64_t req_id, std::vector<uint64_t> *dirty_list_ptr, std::unordered_set<uint64_t> &ignore_set)
    {
        return node_prog_cache.get_cache(t, local_node_handle, req_id, dirty_list_ptr, ignore_set);
    }

    inline node_prog::CacheValueBase*
    graph :: fetch_prog_cache_single(node_prog::prog_type t, uint64_t local_node_handle, uint64_t req_id)
    {
        return node_prog_cache.single_get_cache(t, local_node_handle, req_id);
    }

    inline void
    graph :: insert_prog_cache(node_prog::prog_type t, uint64_t request_id, uint64_t local_node_handle, node_prog::CacheValueBase *toAdd, element::node *n)
    {
        n->add_cached_req(request_id);
        node_prog_cache.put_cache(request_id, t, local_node_handle, toAdd);
    }

    inline void
    graph :: invalidate_prog_cache(uint64_t request_id)
    {
        node_prog_cache.delete_cache(request_id);
    }

    inline void
    graph :: commit_prog_cache(uint64_t req_id)
    {
        node_prog_cache.commit(req_id);
    }

} //namespace db

#endif //__GRAPH__
