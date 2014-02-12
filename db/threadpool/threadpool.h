/*
 * ===============================================================
 *    Description:  Thread pool for shard servers
 *
 *        Created:  01/09/2013 12:00:30 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ================================================================
 */

#ifndef __THREADPOOL__
#define __THREADPOOL__

#include <vector>
#include <deque>
#include <queue>
#include <thread>
#include <po6/threads/mutex.h>
#include <po6/threads/cond.h>

#include "common/weaver_constants.h"
#include "common/message.h"

namespace db
{
class shard;

namespace thread
{
    class pool;
    void worker_thread_loop(pool *tpool, uint64_t);

    class unstarted_thread
    {
        public:
            unstarted_thread(
                uint64_t prio,
                vc::vclock vclk,
                void (*f)(uint64_t, void*),
                void *a);

        public:
            bool operator>(const unstarted_thread &t) const;

        public:
            uint64_t priority;
            vc::vclock vclock;
            void (*func)(uint64_t, void*);
            void *arg;
    };

    inline
    unstarted_thread :: unstarted_thread( 
            uint64_t prio,
            vc::vclock vclk,
            void (*f)(uint64_t, void*),
            void *a)
        : priority(prio)
        , vclock(vclk)
        , func(f)
        , arg(a)
    { }

    // for priority_queue
    struct work_thread_compare 
        : std::binary_function<unstarted_thread*, unstarted_thread*, bool>
    {
        bool operator()(const unstarted_thread* const &r1, const unstarted_thread* const &r2)
        {
            return (r1->priority) > (r2->priority);
        }
    };

    // priority queue type definition
    // each shard server has one such priority queue for each vector timestamper
    typedef std::priority_queue<unstarted_thread*, std::vector<unstarted_thread*>, work_thread_compare> pqueue_t;
    
    class pool
    {
        public:
            int num_threads;
            std::vector<pqueue_t> read_queues;
            std::vector<pqueue_t> write_queues;
            std::vector<vc::vclock_t> last_clocks; // records last transaction vclock pulled of threadpool for that vector timestamper
            vc::qtimestamp_t qts; // queue timestamps
            po6::threads::mutex queue_mutex, thread_loop_mutex;
            po6::threads::cond queue_cond;
            static db::shard *S;
       
        public:
            void add_read_request(uint64_t vt_id, unstarted_thread*);
            void add_write_request(uint64_t vt_id, unstarted_thread*);
            bool check_qts(uint64_t vt_id, uint64_t qts);
            void increment_qts(uint64_t vt_id, uint64_t incr);
            void record_completed_tx(uint64_t vt_id, vc::vclock_t &tx_clk);

        public:
            pool(int n_threads);
    };

    inline
    pool :: pool(int n_threads)
        : num_threads(n_threads)
        , read_queues(NUM_VTS, pqueue_t())
        , write_queues(NUM_VTS, pqueue_t())
        , last_clocks(NUM_VTS, vc::vclock_t(NUM_VTS, 0))
        , qts(NUM_VTS, 0)
        , queue_cond(&queue_mutex)
    {
        uint64_t i;
        std::unique_ptr<std::thread> t;
        for (i = 0; i < (uint64_t)num_threads; i++) {
            t.reset(new std::thread(worker_thread_loop, this, i));
            t->detach();
        }
    }

    inline void
    pool :: add_read_request(uint64_t vt_id, unstarted_thread *t)
    {
        queue_mutex.lock();
        queue_cond.broadcast();
        read_queues[vt_id].push(t);
        queue_mutex.unlock();
    }

    inline void
    pool :: add_write_request(uint64_t vt_id, unstarted_thread *t)
    {
        queue_mutex.lock();
        queue_cond.broadcast();
        write_queues[vt_id].push(t);
        queue_mutex.unlock();
    }

    // check if operation on head of queue corresponding to vt_id
    // is good to go using queue timestamp
    // caution: assuming caller holds queue_mutex
    inline bool
    pool :: check_qts(uint64_t vt_id, uint64_t timestamp)
    {
        return (timestamp == (qts.at(vt_id)+1));
    }

    // increment queue timestamp for a tx which has been ordered
    inline void
    pool :: increment_qts(uint64_t vt_id, uint64_t incr)
    {
        queue_mutex.lock();
        qts[vt_id] += incr;
        queue_cond.broadcast();
        queue_mutex.unlock();
    }

    // record the vclk for last completed write tx
    inline void
    pool :: record_completed_tx(uint64_t vt_id, vc::vclock_t &tx_clk)
    {
        queue_mutex.lock();
        last_clocks[vt_id] = tx_clk;
        queue_cond.broadcast();
        queue_mutex.unlock();
    }
} 
}

#endif
