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

#include "common/weaver_constants.h"
#include "common/property.h"
#include "common/busybee_infra.h"
#include "element/node.h"
#include "element/edge.h"
#include "threadpool/threadpool.h"
#include "cache/program_cache.h"
#include "state/program_state.h"
#include "node_prog/node_prog_type.h"

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
            update_request(enum message::msg_type, uint64_t, uint64_t);
            update_request(enum message::msg_type, uint64_t, std::unique_ptr<message::message>, uint64_t);

        public:
            bool operator>(const update_request &r) const;

        public:
            enum message::msg_type type;
            uint64_t start_time;
            uint64_t update_count; // for transit update requests (migration)
            std::unique_ptr<message::message> msg;
    };

    inline
    update_request :: update_request(enum message::msg_type mt, uint64_t st, uint64_t uc=0)
        : type(mt)
        , start_time(st)
        , update_count(uc)
    {
    }

    inline
    update_request :: update_request(enum message::msg_type mt, uint64_t st, std::unique_ptr<message::message> m, uint64_t uc=0)
        : type(mt)
        , start_time(st)
        , update_count(uc)
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
            uint64_t cur_node, // node being migrated
                prev_node; // last node which was migrated (used for permanent deletion of migrated nodes)
            uint64_t new_loc; // shard to which this node is being migrated
            std::vector<std::unique_ptr<message::message>> pending_requests; // queued requests
            uint64_t migr_clock; // clock at which migration started
            uint64_t other_clock;
            uint8_t start_step4; // step 4 can be started only when both coordinator and other shard
                                 // send their acks, i.e. this count reaches 2
            uint8_t start_next_round;   // this migration is finished when all pending requests have been forwarded
                                        // and all in-nbrs have been updated, i.e. this count reaches 2
            
            migrate_request()
                : cur_node(0)
                , prev_node(0)
                , new_loc(-1)
                , migr_clock(0)
                , other_clock(0)
                , start_step4(0)
                , start_next_round(0)
            {
            }
    };

    class graph;
    typedef void (*graph_arg_func)(graph*);

    class graph
    {
        public:
            graph(uint64_t my_id);

        private:
            po6::threads::mutex clock_mutex, update_mutex, deletion_mutex, prog_mutex;
            uint64_t my_clock;

        public:
            // Consistency
            bool check_clock(uint64_t time);
            bool increment_clock();
            uint64_t get_clock();
            element::node* acquire_node(uint64_t node_handle);
            void release_node(element::node *n);

            // Graph state
            std::unordered_map<uint64_t, element::node*> nodes;
            uint64_t myid;
            db::thread::pool thread_pool;
            void create_node(uint64_t time, bool migrate);
            std::pair<uint64_t, std::unique_ptr<std::vector<uint64_t>>> delete_node(uint64_t node, uint64_t del_time);
            uint64_t create_edge(uint64_t local_node, uint64_t time, uint64_t remote_node, uint64_t remote_loc, uint64_t remote_time);
            uint64_t create_reverse_edge(uint64_t time, uint64_t local_node, uint64_t remote_node, uint64_t remote_loc);
            std::pair<uint64_t, std::unique_ptr<std::vector<uint64_t>>> delete_edge(uint64_t node, uint64_t edge_handle, uint64_t del_time);
            uint64_t add_edge_property(uint64_t n, uint64_t e, common::property &prop);
            std::pair<uint64_t, std::unique_ptr<std::vector<uint64_t>>> delete_all_edge_property(uint64_t n, uint64_t e, uint32_t key, uint64_t del_time);
            void permanent_delete(uint64_t req_id, uint64_t migr_del_id);
            void permanent_node_delete(element::node *n);
            void permanent_edge_delete(uint64_t n, uint64_t e);
            uint64_t get_node_count();

            // Node migration
            migrate_request mrequest; // pending migration request object
            po6::threads::mutex migration_mutex, msg_count_mutex;
            uint64_t migr_node; // newly migrated node
            std::priority_queue<update_request*, std::vector<update_request*>, req_compare> pending_updates;
            uint64_t pending_update_count;
            uint64_t current_update_count;
            uint64_t pending_edge_updates;
            uint64_t target_clock, new_shard_target_clock;
            bool migrated, migr_token;
            timespec migr_time;
            graph_arg_func migr_func;
            std::vector<uint64_t> request_count, request_count_const, node_count;
            uint32_t request_reply_count;
            uint64_t cur_node_count;
            std::unordered_map<uint64_t, uint32_t> agg_msg_count;
            std::deque<std::pair<uint64_t, uint32_t>> sorted_nodes;
            std::deque<std::unique_ptr<std::pair<uint64_t, uint64_t>>> migrated_nodes; // for permanent deletion later on
            void update_migrated_nbr_nonlocking(element::node *n, uint64_t edge, uint64_t rnode, uint64_t new_loc);
            void update_migrated_nbr(uint64_t lnode, uint64_t edge, uint64_t rnode, uint64_t new_loc);
            bool set_target_clock(uint64_t time);
            bool set_callback(uint64_t time, graph_arg_func mfunc);
            void set_update_count(uint64_t pending_count, uint64_t current_count, uint64_t target_clock);
            void fwd_transit_node_update(std::unique_ptr<message::message> msg);
            void delete_migrated_node(uint64_t migr_node);
            
            // Permanent deletion
            uint64_t del_id;
            std::deque<std::unique_ptr<perm_del>> delete_req_ids;
            
            // Messaging infrastructure
            std::shared_ptr<po6::net::location> myloc;
            std::shared_ptr<po6::net::location> myrecloc;
            busybee_mta *bb; // Busybee instance used for sending and receiving messages
            busybee_returncode send(uint64_t loc, std::auto_ptr<e::buffer> buf);
            
#ifdef __WEAVER_DEBUG__
            // testing
            std::unordered_map<uint64_t, uint64_t> req_count;
            po6::threads::mutex test_mutex;
            uint64_t sent_count, rec_count;
#endif

            // Node programs
            // prog_type-> map from request_id to map from node handle to request state for that node
            state::program_state node_prog_req_state; 
            bool prog_req_state_exists(node_prog::prog_type t, uint64_t request_id, uint64_t local_node_handle);
            node_prog::Packable_Deletable* fetch_prog_req_state(node_prog::prog_type t, uint64_t request_id, uint64_t local_node_handle);
            void insert_prog_req_state(node_prog::prog_type t, uint64_t request_id, uint64_t local_node_handle, node_prog::Packable_Deletable *toAdd);
            // prog_type-> map from node handle to map from request_id to cache values -- used to do cache read/updates
            cache::program_cache node_prog_cache;
            bool prog_cache_exists(node_prog::prog_type t, uint64_t local_node_handle, uint64_t req_id);
            std::vector<std::shared_ptr<node_prog::CacheValueBase>> fetch_prog_cache(node_prog::prog_type t, uint64_t local_node_handle, uint64_t req_id, std::vector<uint64_t> *dirty_list_ptr, std::unordered_set<uint64_t> &ignore_set);
            std::shared_ptr<node_prog::CacheValueBase> fetch_prog_cache_single(node_prog::prog_type t, uint64_t local_node_handle, uint64_t req_id);
            void insert_prog_cache(node_prog::prog_type t, uint64_t request_id, uint64_t local_node_handle, std::shared_ptr<node_prog::CacheValueBase> toAdd, element::node *n);
            void invalidate_prog_cache(uint64_t request_id);
            void commit_prog_cache(uint64_t req_id);
            void print_cache_size();
            // completed node programs
            std::unordered_set<uint64_t> done_ids; // TODO clean up of done_ids
            void add_done_request(std::vector<std::pair<uint64_t, node_prog::prog_type>> &completed_requests);
            bool check_done_request(uint64_t req_id);
            void clear_req_use(uint64_t req_id);
    };

    inline
    graph :: graph(uint64_t my_id)
        : my_clock(0)
        , myid(my_id)
        , thread_pool(NUM_THREADS-1)
        , pending_update_count(MAX_TIME)
        , current_update_count(MAX_TIME)
        , pending_edge_updates(0)
        , target_clock(0)
        , new_shard_target_clock(0)
        , migrated(false)
        , migr_token(false)
        , request_count(NUM_SHARDS, 0)
        , node_count(NUM_SHARDS, 0)
        , request_reply_count(0)
        , cur_node_count(0)
#ifdef __WEAVER_DEBUG__
        , sent_count(0)
        , rec_count(0)
#endif
        , node_prog_req_state(&prog_mutex)
    {
        initialize_busybee(bb, myid, myloc);
        message::prog_state = &node_prog_req_state;
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

    inline uint64_t
    graph :: get_clock()
    {
        uint64_t clk;
        clock_mutex.lock();
        clk = my_clock;
        clock_mutex.unlock();
        return clk;
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
            n->waiters++;
            while (n->in_use) {
                n->cv.wait();
            }
            n->waiters--;
            n->in_use = true;
        }
        update_mutex.unlock();
        return n;
    }

    inline void
    graph :: release_node(element::node *n)
    {
        update_mutex.lock();
        n->in_use = false;
        if (n->waiters > 0) {
            n->cv.signal();
            update_mutex.unlock();
        } else if (n->permanently_deleted) {
            uint64_t node_handle = n->get_creat_time();
            nodes.erase(node_handle);
            cur_node_count--;
            update_mutex.unlock();
            msg_count_mutex.lock();
            agg_msg_count.erase(node_handle);
            msg_count_mutex.unlock();
            permanent_node_delete(n);
        } else {
            update_mutex.unlock();
        }
        // TODO delete state and cache corresponding to this node
    }

    inline void
    graph :: create_node(uint64_t time, bool migrate = false)
    {
        element::node *new_node = new element::node(time, &update_mutex);
        update_mutex.lock();
        if (!nodes.emplace(time, new_node).second) {
            DEBUG << "node already exists in node map!" << std::endl;
        }
        cur_node_count++;
        update_mutex.unlock();
        if (migrate) {
            migration_mutex.lock();
            migr_node = time;
            migration_mutex.unlock();
        } else {
            new_node->state = element::node::mode::STABLE;
            for (uint64_t i = 0; i < NUM_SHARDS; i++) {
                new_node->prev_locs.emplace_back(0);
            }
            new_node->prev_locs.at(myid-1) = 1;
        }
        release_node(new_node);
    }

    inline std::pair<uint64_t, std::unique_ptr<std::vector<uint64_t>>>
    graph :: delete_node(uint64_t node_handle, uint64_t del_time)
    {
        std::pair<uint64_t, std::unique_ptr<std::vector<uint64_t>>> ret;
        element::node *n = acquire_node(node_handle);
        ret.first = ++n->update_count;
        if (n->state != element::node::mode::IN_TRANSIT) {
            n->update_del_time(del_time);
            n->updated = true;
            ret.first = 0;
        }
        ret.second = std::move(n->purge_cache());
        for (auto rid: *ret.second) {
            invalidate_prog_cache(rid);
        }
        release_node(n);
        if (ret.first == 0) {
            deletion_mutex.lock();
            delete_req_ids.emplace_back(std::unique_ptr<perm_del>(
                new perm_del(message::NODE_DELETE_REQ, del_time, node_handle)));
            deletion_mutex.unlock();
        }
        return ret;
    }

    inline uint64_t
    graph :: create_edge(uint64_t local_node, uint64_t time, 
        uint64_t remote_node, uint64_t remote_loc, uint64_t remote_time)
    {
        uint64_t ret;
        element::node *n = acquire_node(local_node);
        ret = ++n->update_count;
        if (n->state == element::node::mode::IN_TRANSIT) {
            release_node(n);
        } else {
            element::edge *new_edge = new element::edge(time, remote_loc, remote_node);
            n->add_edge(new_edge, true);
            n->updated = true;
            release_node(n);
            message::message msg(message::REVERSE_EDGE_CREATE);
            message::prepare_message(msg, message::REVERSE_EDGE_CREATE,
                remote_time, time, local_node, myid, remote_node);
            send(remote_loc, msg.buf);
            ret = 0;
        }
        return ret;
    }

    inline uint64_t
    graph :: create_reverse_edge(uint64_t time, uint64_t local_node, uint64_t remote_node, uint64_t remote_loc)
    {
        uint64_t ret;
        element::node *n = acquire_node(local_node);
        ret = ++n->update_count;
        if (n->state != element::node::mode::IN_TRANSIT) {
            element::edge *new_edge = new element::edge(time, remote_loc, remote_node);
            n->add_edge(new_edge, false);
            n->updated = true;
            ret = 0;
        }
        release_node(n);
        return ret;
    }

    inline std::pair<uint64_t, std::unique_ptr<std::vector<uint64_t>>>
    graph :: delete_edge(uint64_t node_handle, uint64_t edge_handle, uint64_t del_time)
    {
        std::pair<uint64_t, std::unique_ptr<std::vector<uint64_t>>> ret;
        element::node *n = acquire_node(node_handle);
        ret.first = ++n->update_count;
        if (n->state != element::node::mode::IN_TRANSIT) {
            element::edge *e = n->out_edges.at(edge_handle);
            e->update_del_time(del_time);
            n->updated = true;
            n->dependent_del++;
            ret.first = 0;
        }
        ret.second = std::move(n->purge_cache());
        for (auto rid: *ret.second) {
            invalidate_prog_cache(rid);
        }
        release_node(n);
        if (ret.first == 0) {
            deletion_mutex.lock();
            delete_req_ids.emplace_back(std::unique_ptr<perm_del>(
                new perm_del(message::EDGE_DELETE_REQ, del_time, node_handle, edge_handle)));
            deletion_mutex.unlock();
        }
        return ret;
    }

    inline uint64_t
    graph :: add_edge_property(uint64_t node, uint64_t edge, common::property &prop)
    {
        element::node *n = acquire_node(node);
        element::edge *e = n->out_edges.at(edge);
        uint64_t ret = ++n->update_count;
        if (n->state != element::node::mode::IN_TRANSIT) {
            e->add_property(prop);
            n->updated = true;
            ret = 0;
        }
        release_node(n);
        return ret;
    }

    inline std::pair<uint64_t, std::unique_ptr<std::vector<uint64_t>>>
    graph :: delete_all_edge_property(uint64_t node, uint64_t edge, uint32_t key, uint64_t time)
    {
        element::node *n = acquire_node(node);
        element::edge *e = n->out_edges.at(edge);
        std::pair<uint64_t, std::unique_ptr<std::vector<uint64_t>>> ret;
        ret.first = ++n->update_count;
        if (n->state != element::node::mode::IN_TRANSIT) {
            e->delete_property(key, time);
            n->updated = true;
            n->dependent_del++;
            ret.first = 0;
        }
        ret.second = std::move(n->purge_cache());
        for (auto rid: *ret.second) {
            invalidate_prog_cache(rid);
        }
        release_node(n);
        if (ret.first == 0) {
            deletion_mutex.lock();
            delete_req_ids.emplace_back(std::unique_ptr<perm_del>(
                new perm_del(message::EDGE_DELETE_PROP, time, node, edge, key)));
            deletion_mutex.unlock();
        }
        return ret;
    }

    inline void
    graph :: permanent_delete(uint64_t req_id, uint64_t migr_del_id)
    {
        std::deque<std::unique_ptr<perm_del>> delete_req_ids_copy;
        std::deque<std::unique_ptr<std::pair<uint64_t, uint64_t>>> migrated_nodes_copy;
        // permanently delete migrated nodes
        mrequest.mutex.lock();
        while (!migrated_nodes.empty()) {
            if (migrated_nodes.front()->first >= migr_del_id) {
                break;
            } else {
                migrated_nodes_copy.emplace_back(std::move(migrated_nodes.front()));
                migrated_nodes.pop_front();
            }
        }
        mrequest.mutex.unlock();
        while (!migrated_nodes_copy.empty()) {
            delete_migrated_node(migrated_nodes_copy.front()->second);
            migrated_nodes_copy.pop_front();
        }

        // permanently delete deleted nodes
        deletion_mutex.lock();
        del_id = req_id;
        while (!delete_req_ids.empty()) {
            if (delete_req_ids.front()->req_id <= req_id) {
                // can delete this element, place on copy list
                delete_req_ids_copy.emplace_back(std::move(delete_req_ids.front()));
                delete_req_ids.pop_front();
            } else {
                break;
            }
        }
        deletion_mutex.unlock();
        while (!delete_req_ids_copy.empty()) {
            perm_del &front = *delete_req_ids_copy.front();
            switch (front.type) {
                case message::NODE_DELETE_REQ: {
                    message::message msg;
                    auto &req = front.request.del_node;
                    element::node *n;
                    n = acquire_node(req.node_handle);
                    n->permanently_deleted = true; // actual deletion in release_node()
                    release_node(n);
                    break;
                }
                case message::EDGE_DELETE_REQ: {
                    message::message msg;
                    auto &req = front.request.del_edge;
                    element::node *n = acquire_node(req.node_handle);
                    if (n->out_edges.find(req.edge_handle) != n->out_edges.end()) {
                        element::edge *e = n->out_edges.at(req.edge_handle);
                        message::prepare_message(msg, message::PERMANENT_DELETE_EDGE, e->nbr.handle, req.edge_handle);
                        send(e->nbr.loc, msg.buf);
                        n->out_edges.erase(req.edge_handle);
                        delete e;
                    } else {
                        DEBUG << "Not found edge in perm_del" << std::endl;
                    }
                    n->dependent_del--;
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
                    n->dependent_del--;
                    release_node(n);
                    break;
                }
                default:
                    std::cerr << "Bad type in permanent delete list " << front.type << std::endl;
            }
            delete_req_ids_copy.pop_front();
        }
        message::message msg;
        message::prepare_message(msg, message::CLEAN_UP_ACK); 
        send(COORD_ID, msg.buf);
    }

    // caution: assuming caller holds update_mutex
    inline void
    graph :: permanent_node_delete(element::node *n)
    {
        element::edge *e;
        message::message msg;
        // following 2 loops are no-ops in case of deletion of
        // migrated nodes, since edges have already been deleted
        for (auto &it: n->out_edges) {
            e = it.second;
            message::prepare_message(msg, message::PERMANENT_DELETE_EDGE, e->nbr.handle, it.first);
            send(e->nbr.loc, msg.buf);
            delete e;
        }
        for (auto &it: n->in_edges) {
            e = it.second;
            message::prepare_message(msg, message::PERMANENT_DELETE_EDGE, e->nbr.handle, it.first);
            send(e->nbr.loc, msg.buf);
            delete e;
        }
        delete n;
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
            DEBUG << "Not found node " << node_handle << " in perm_edge_del" << std::endl;
            deletion_mutex.unlock();
            return;
        }
        if (n->in_edges.find(edge_handle) != n->in_edges.end()) {
            e = n->in_edges.at(edge_handle);
            n->in_edges.erase(edge_handle);
            delete e;
        } else if (n->out_edges.find(edge_handle) != n->out_edges.end()) {
            e = n->out_edges.at(edge_handle);
            n->out_edges.erase(edge_handle);
            delete e;
        }
        release_node(n);
        deletion_mutex.unlock();
    }

    inline uint64_t
    graph :: get_node_count()
    {
        uint64_t to_ret;
        update_mutex.lock();
        to_ret = cur_node_count;
        update_mutex.unlock();
        return to_ret;
    }

    // migration methods

    inline void
    graph :: update_migrated_nbr_nonlocking(element::node *n, uint64_t edge_handle, uint64_t remote_node, uint64_t new_loc)
    {
        bool found = false;
        if (n->out_edges.find(edge_handle) != n->out_edges.end()) {
            element::edge *e = n->out_edges.at(edge_handle);
            assert(e->nbr.handle == remote_node);
            n->msg_count[e->nbr.loc-1] = 0;
            e->msg_count = 0;
            e->nbr.loc = new_loc;
            found = true;
        }
        if (n->in_edges.find(edge_handle) != n->in_edges.end()) {
            element::edge *e = n->in_edges.at(edge_handle);
            assert(e->nbr.handle == remote_node);
            n->msg_count[e->nbr.loc-1] = 0;
            e->msg_count = 0;
            e->nbr.loc = new_loc;
            found = true;
        }
        if (!found) {
            DEBUG << "Neighbor not found for migrated node " << remote_node << std::endl;
        }
    }

    inline void
    graph :: update_migrated_nbr(uint64_t local_node, uint64_t edge_handle, uint64_t remote_node, uint64_t new_loc)
    {
        element::node *n = acquire_node(local_node);
        update_migrated_nbr_nonlocking(n, edge_handle, remote_node, new_loc);
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

    inline bool
    graph :: set_callback(uint64_t time, graph_arg_func mfunc)
    {
        bool ret;
        clock_mutex.lock();
        if (time == my_clock) {
            ret = true;
        } else {
            target_clock = time;
            migr_func = mfunc;
            ret = false;
        }
        clock_mutex.unlock();
        return ret;
    }

    inline void
    graph :: set_update_count(uint64_t pending_count, uint64_t current_count, uint64_t tclock)
    {
        migration_mutex.lock();
        pending_update_count = pending_count;
        current_update_count = current_count;
        new_shard_target_clock = tclock;
        migration_mutex.unlock();
    }

    inline void
    graph :: fwd_transit_node_update(std::unique_ptr<message::message> msg)
    {
        mrequest.mutex.lock();
        send(mrequest.new_loc, msg->buf);
        mrequest.mutex.unlock();
    }

    inline void
    graph :: delete_migrated_node(uint64_t migr_node)
    {
        element::node *n;
        n = acquire_node(migr_node);
        n->permanently_deleted = true;
        // deleting edges now so as to prevent sending messages to neighbors for permanent edge deletion
        // rest of deletion happens in release_node()
        for (auto &e: n->out_edges) {
            delete e.second;
        }
        for (auto &e: n->in_edges) {
            delete e.second;
        }
        n->out_edges.clear();
        n->in_edges.clear();
        assert(n->waiters == 0); // nobody should try to acquire this node now
        release_node(n);
    }

    // messaging methods
   
    inline busybee_returncode
    graph :: send(uint64_t loc, std::auto_ptr<e::buffer> msg)
    {
        busybee_returncode ret;
        if ((ret = bb->send(loc, msg)) != BUSYBEE_SUCCESS) {
            std::cerr << "msg send error: " << ret << std::endl;
        }
        return ret;
    }

    // work loop for threads in thread pool
    // pull request off job priority queue, check if it is okay to run
    // if yes, execute, else sleep and wait for clock to be incremented
    void
    thread :: worker_thread_loop(thread::pool *tpool)
    {
        thread::unstarted_thread *thr = NULL;
        std::priority_queue<thread::unstarted_thread*, 
            std::vector<unstarted_thread*>, 
            thread::work_thread_compare> &tq = tpool->work_queue;
        po6::threads::mutex &m = tpool->queue_mutex;
        po6::threads::cond &c = tpool->work_queue_cond;
        bool can_start = false;
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

    inline node_prog::Packable_Deletable*
    graph :: fetch_prog_req_state(node_prog::prog_type t, uint64_t request_id, uint64_t local_node_handle)
    {
        return node_prog_req_state.get_state(t, request_id, local_node_handle);
    }

    inline void 
    graph :: insert_prog_req_state(node_prog::prog_type t, uint64_t request_id, uint64_t local_node_handle,
        node_prog::Packable_Deletable* toAdd)
    {
        node_prog_req_state.put_state(t, request_id, local_node_handle, toAdd);
    }

    inline bool
    graph :: prog_cache_exists(node_prog::prog_type t, uint64_t node_handle, uint64_t req_id)
    {
        return node_prog_cache.cache_exists(t, node_handle, req_id);
    }

    inline std::vector<std::shared_ptr<node_prog::CacheValueBase>>
    graph :: fetch_prog_cache(node_prog::prog_type t, uint64_t local_node_handle, uint64_t req_id,
        std::vector<uint64_t> *dirty_list_ptr, std::unordered_set<uint64_t> &ignore_set)
    {
try {
        return node_prog_cache.get_cache(t, local_node_handle, req_id, dirty_list_ptr, ignore_set);
} catch (const std::out_of_range &e) {
    DEBUG << "caught exception here" << std::endl;
    while(1);
}
    }

    inline std::shared_ptr<node_prog::CacheValueBase>
    graph :: fetch_prog_cache_single(node_prog::prog_type t, uint64_t local_node_handle, uint64_t req_id)
    {
        return node_prog_cache.single_get_cache(t, local_node_handle, req_id);
    }

    inline void
    graph :: insert_prog_cache(node_prog::prog_type t, uint64_t request_id, uint64_t local_node_handle,
        std::shared_ptr<node_prog::CacheValueBase> toAdd, element::node *n)
    {
try {
        n->add_cached_req(request_id);
} catch (const std::out_of_range &e) {
    DEBUG << "caught exception here" << std::endl;
    while(1);
}
try {
        node_prog_cache.put_cache(request_id, t, local_node_handle, toAdd);
} catch (const std::out_of_range &e) {
    DEBUG << "caught exception here" << std::endl;
    while(1);
}
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
    
    inline void
    graph :: print_cache_size()
    {
        node_prog_cache.print_size();
    }

    inline void
    graph :: add_done_request(std::vector<std::pair<uint64_t, node_prog::prog_type>> &completed_requests)
    {
        prog_mutex.lock();
        for (auto &p: completed_requests) {
            done_ids.emplace(p.first);
        }
        for (auto &p: completed_requests) {
            node_prog_req_state.delete_req_state(p.first, p.second);
        }
        prog_mutex.unlock();
    }

    inline bool
    graph :: check_done_request(uint64_t req_id)
    {
        bool ret;
        prog_mutex.lock();
        ret = (done_ids.find(req_id) != done_ids.end());
        if (!ret) {
            node_prog_req_state.set_in_use(req_id);
        }
        prog_mutex.unlock();
        return ret;
    }

    inline void
    graph :: clear_req_use(uint64_t req_id)
    {
        prog_mutex.lock();
        node_prog_req_state.clear_in_use(req_id);
        prog_mutex.unlock();
    }

} //namespace db

#endif //__GRAPH__
