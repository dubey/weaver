/*
 * ===============================================================
 *    Description:  Graph state corresponding to the partition
 *                  stored on this shard server.
 *
 *        Created:  07/25/2013 12:46:05 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __SHARD_SERVER__
#define __SHARD_SERVER__

#include <vector>
#include <unordered_map>
#include <po6/threads/mutex.h>
#include <po6/net/location.h>

#include "common/weaver_constants.h"
#include "common/vclock.h"
#include "common/message.h"
#include "common/busybee_infra.h"
#include "common/event_order.h"
#include "element/node.h"
#include "element/edge.h"
#include "threadpool/threadpool.h"

namespace db
{
    // state for a deleted graph element which is yet to be permanently deleted
    struct perm_del
    {
        enum message::msg_type type;
        vc::vclock_t tdel;
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

        inline
        perm_del(enum message::msg_type t, vc::vclock_t &td, uint64_t node)
            : type(t)
            , tdel(td)
        {
            request.del_node.node_handle = node;
        }

        inline
        perm_del(enum message::msg_type t, vc::vclock_t &td, uint64_t node, uint64_t edge)
            : type(t)
            , tdel(td)
        {
            request.del_edge.node_handle = node;
            request.del_edge.edge_handle = edge;
        }
        
        inline
        perm_del(enum message::msg_type t, vc::vclock_t &td, uint64_t node, uint64_t edge, uint32_t k)
            : type(t)
            , tdel(td)
        {
            request.del_edge_prop.node_handle = node;
            request.del_edge_prop.edge_handle = edge;
            request.del_edge_prop.key = k;
        }
    };

    // Pending update request
    class graph_request
    {
        public:
            graph_request(enum message::msg_type, std::unique_ptr<message::message>);

        public:
            enum message::msg_type type;
            std::unique_ptr<message::message> msg;
    };

    inline
    graph_request :: graph_request(enum message::msg_type mt, std::unique_ptr<message::message> m)
        : type(mt)
        , msg(std::move(m))
    { }

    // graph partition state and associated data structures
    class shard
    {
        public:
            shard(uint64_t my_id);

            // Mutexes
        private:
            po6::threads::mutex update_mutex // shard update mutex
                , clock_mutex // vector clock/queue timestamp mutex
                , edge_map_mutex
                , msg_count_mutex
                , migration_mutex;
        public:
            po6::threads::mutex queue_mutex; // exclusive access to thread pool queues

            // Consistency
        public:
            void record_completed_transaction(uint64_t vt_id, uint64_t transaction_completed_id);
            element::node* acquire_node(uint64_t node_handle);
            void release_node(element::node *n);

            // Graph state
            uint64_t shard_id;
        private:
            std::unordered_map<uint64_t, element::node*> nodes; // node handle -> ptr to node object
            std::unordered_map<uint64_t, uint64_t> edges; // edge handle -> node handle
            db::thread::pool thread_pool;
        public:
            void add_write_request(uint64_t vt_id, thread::unstarted_thread *thr);
            void add_read_request(uint64_t vt_id, thread::unstarted_thread *thr);
            void create_node(uint64_t node_handle, vc::vclock_t &vclk, bool migrate);
            uint64_t delete_node(uint64_t node_handle, vc::vclock_t &vclk);
            uint64_t create_edge(uint64_t edge_handle, uint64_t local_node,
                    uint64_t remote_node, uint64_t remote_loc, vc::vclock_t &vclk);
            uint64_t create_reverse_edge(uint64_t edge_handle, uint64_t local_node,
                    uint64_t remote_node, uint64_t remote_loc, vc::vclock_t &vclk);
            uint64_t delete_edge(uint64_t edge_handle, vc::vclock_t &vclk);
            uint64_t get_node_count();

            // Permanent deletion
        private:
            std::deque<perm_del> pending_deletes;

            // Migration
        public:
            uint64_t cur_node_count;
            std::unordered_map<uint64_t, uint32_t> agg_msg_count;

            // Messaging infrastructure
        public:
            std::shared_ptr<po6::net::location> myloc;
            busybee_mta *bb; // Busybee instance used for sending and receiving messages
            busybee_returncode send(uint64_t loc, std::auto_ptr<e::buffer> buf);
    };

    inline
    shard :: shard(uint64_t my_id)
        : shard_id(my_id)
        , thread_pool(NUM_THREADS - 1)
        , cur_node_count(0)
    {
        thread::pool::S = this;
        initialize_busybee(bb, shard_id, myloc);
        order::kronos_cl = chronos_client_create(KRONOS_IPADDR, KRONOS_PORT, KRONOS_NUM_SHARDS);
        assert(NUM_SHARDS == KRONOS_NUM_SHARDS);
    }

    // Consistency methods

    inline void
    shard :: record_completed_transaction(uint64_t vt_id, uint64_t transaction_completed_id)
    {
        thread_pool.record_completed_transaction(vt_id, transaction_completed_id);
    }

    // find the node corresponding to given handle
    // lock and return the node
    // return NULL if node does not exist (possibly permanently deleted)
    inline element::node*
    shard :: acquire_node(uint64_t node_handle)
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

    // unlock the previously acquired node, and wake any waiting threads
    inline void
    shard :: release_node(element::node *n)
    {
        update_mutex.lock();
        n->in_use = false;
        if (n->waiters > 0) {
            n->cv.signal();
            update_mutex.unlock();
        } else if (n->permanently_deleted) {
            // TODO
            uint64_t node_handle = n->get_handle();
            nodes.erase(node_handle);
            cur_node_count--;
            update_mutex.unlock();
            msg_count_mutex.lock();
            agg_msg_count.erase(node_handle);
            msg_count_mutex.unlock();
            //permanent_node_delete(n);
        } else {
            update_mutex.unlock();
        }
    }


    // Graph state update methods

    inline void
    shard :: add_write_request(uint64_t vt_id, thread::unstarted_thread *thr)
    {
        thread_pool.add_write_request(vt_id, thr);
    }

    inline void
    shard :: add_read_request(uint64_t vt_id, thread::unstarted_thread *thr)
    {
        thread_pool.add_read_request(vt_id, thr);
    }

    inline void
    shard :: create_node(uint64_t node_handle, vc::vclock_t &vclk, bool migrate)
    {
        element::node *new_node = new element::node(node_handle, vclk, &update_mutex);
        update_mutex.lock();
        if (!nodes.emplace(node_handle, new_node).second) {
            DEBUG << "node already exists in node map!" << std::endl;
        }
        cur_node_count++;
        update_mutex.unlock();
        if (migrate) {
            // TODO
            migration_mutex.lock();
            //migr_node = node_handle;
            migration_mutex.unlock();
        } else {
            new_node->state = element::node::mode::STABLE;
            for (uint64_t i = 0; i < NUM_SHARDS; i++) {
                new_node->prev_locs.emplace_back(0);
            }
            new_node->prev_locs.at(shard_id-SHARD_ID_INCR) = 1;
        }
        release_node(new_node);
    }

    inline uint64_t
    shard :: delete_node(uint64_t node_handle, vc::vclock_t &tdel)
    {
        uint64_t ret;
        element::node *n = acquire_node(node_handle);
        ret = ++n->update_count;
        if (n->state != element::node::mode::IN_TRANSIT) {
            n->update_del_time(tdel);
            n->updated = true;
            ret = 0;
        }
        release_node(n);
        // TODO permanent deletion
        return ret;
    }

    inline uint64_t
    shard :: create_edge(uint64_t edge_handle, uint64_t local_node,
            uint64_t remote_node, uint64_t remote_loc, vc::vclock_t &vclk)
    {
        uint64_t ret;
        element::node *n = acquire_node(local_node);
        ret = ++n->update_count;
        if (n->state == element::node::mode::IN_TRANSIT) {
            release_node(n);
        } else {
            element::edge *new_edge = new element::edge(edge_handle, vclk,
                    remote_loc, remote_node);
            n->add_edge(new_edge, true);
            n->updated = true;
            release_node(n);
            edge_map_mutex.lock();
            edges.emplace(edge_handle, local_node);
            edge_map_mutex.unlock();
            message::message msg;
            message::prepare_message(msg, message::REVERSE_EDGE_CREATE,
                vclk, edge_handle, remote_node, local_node, shard_id);
            send(remote_loc, msg.buf);
            ret = 0;
        }
        return ret;
    }

    inline uint64_t
    shard :: create_reverse_edge(uint64_t edge_handle, uint64_t local_node,
            uint64_t remote_node, uint64_t remote_loc, vc::vclock_t &vclk)
    {
        uint64_t ret;
        element::node *n = acquire_node(local_node);
        ret = ++n->update_count;
        if (n->state != element::node::mode::IN_TRANSIT) {
            element::edge *new_edge = new element::edge(edge_handle, vclk,
                    remote_loc, remote_node);
            n->add_edge(new_edge, false);
            n->updated = true;
            ret = 0;
        }
        release_node(n);
        if (ret == 0) {
            edge_map_mutex.lock();
            edges.emplace(edge_handle, local_node);
            edge_map_mutex.unlock();
        }
        return ret;
    }

    inline uint64_t
    shard :: delete_edge(uint64_t edge_handle, vc::vclock_t &tdel)
    {
        uint64_t ret;
        edge_map_mutex.lock();
        uint64_t node_handle = edges.at(edge_handle);
        edge_map_mutex.unlock();
        element::node *n = acquire_node(node_handle);
        ret = ++n->update_count;
        if (n->state != element::node::mode::IN_TRANSIT) {
            element::edge *e = n->out_edges.at(edge_handle);
            e->update_del_time(tdel);
            n->updated = true;
            n->dependent_del++;
            ret = 0;
        }
        release_node(n);
        // TODO permanent deletion
        return ret;
    }

    // messaging methods

    inline busybee_returncode
    shard :: send(uint64_t loc, std::auto_ptr<e::buffer> msg)
    {
        busybee_returncode ret;
        if ((ret = bb->send(loc, msg)) != BUSYBEE_SUCCESS) {
            std::cerr << "msg send error: " << ret << std::endl;
        }
        return ret;
    }

    // work loop for threads in thread pool
    // check all queues are ready to go
    // if yes, execute the earliest job, else sleep and wait for incoming jobs
    // "earliest" is decided by comparison functions using vector clocks and Kronos
    void
    thread :: worker_thread_loop(thread::pool *tpool)
    {
        thread::unstarted_thread *thr = NULL;
        std::vector<thread::pqueue_t> &read_queues = tpool->read_queues;
        std::vector<thread::pqueue_t> &write_queues = tpool->write_queues;
        po6::threads::cond &c = tpool->queue_cond;
        std::vector<vc::vclock_t> timestamps(NUM_VTS, vc::vclock_t());
        std::vector<uint64_t> &last_ids = tpool->last_ids;
        bool read = false;
        while (true) {
            tpool->thread_loop_mutex.lock(); // only one thread accesses queues
            // TODO add job method should be non-blocking on this mutex
            tpool->queue_mutex.lock(); // prevent more jobs from being added
            // first check reads, TODO: maybe change to prevent starvation (note reads never have to go to kronos or wait for full queues)
            DEBUG << "checking read queues" << std::endl;
            for (uint64_t vt_id = 0; vt_id < NUM_VTS; vt_id++) {
                thread::pqueue_t &pq = read_queues.at(vt_id);
                if (!pq.empty() && pq.top()->priority < last_ids.at(vt_id)) {
                    DEBUG << "read queue " << vt_id << " has node prog that can be run" << std::endl;
                    thr = read_queues.at(vt_id).top();
                    read_queues.at(vt_id).pop();
                    read = true;
                    break;
                }
            }
            if (!read) {
                // get next jobs from each queue
                DEBUG << "going to collect jobs from write queues" << std::endl;
                for (uint64_t vt_id = 0; vt_id < NUM_VTS; vt_id++) {
                    thread::pqueue_t &pq = write_queues.at(vt_id);
                    // wait for queue to receive at least one job
                    DEBUG << "waiting for queue to fill" << std::endl;
                    while (pq.empty()) {
                        c.wait();
                    }
                    thr = pq.top();
                    // check for correct ordering of queue timestamp (which is priority for thread)
                    DEBUG << "waiting for qts to increment" << std::endl;
                    while (!tpool->check_qts(vt_id, thr->priority)) {
                        DEBUG << "sleeping, qts reqd = " << thr->priority << std::endl;
                        c.wait();
                    }
                    DEBUG << "done checks" << std::endl;
                }
                // all write queues are good to go, compare timestamps
                for (uint64_t vt_id = 0; vt_id < NUM_VTS; vt_id++) {
                    timestamps.at(vt_id) = write_queues.at(vt_id).top()->vclock;
                }
                DEBUG << "going to compare vt" << std::endl;
                uint64_t exec_vt_id = (NUM_VTS==1)? 0:order::compare_vts(timestamps);
                thr = write_queues.at(exec_vt_id).top();
                write_queues.at(exec_vt_id).pop();
                // TODO check nop
            }
            tpool->queue_mutex.unlock();
            tpool->thread_loop_mutex.unlock();
            (*thr->func)(thr->arg);
            read = false;
            // queue timestamp is incremented by the thread, upon finishing
            // because the decision to increment or not is based on thread-specific knowledge
            // moreover, when to increment can also be decided by thread only
            // this could potentially decrease throughput, because other ops in the
            // threadpool are blocked, waiting for this thread to increment qts
            delete thr;
        }
    }
}

#endif
