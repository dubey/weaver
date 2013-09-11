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

#include "common/message.h"

namespace db
{
class shard;

namespace thread
{
    class pool;
    void worker_thread_loop(pool *tpool);

    class unstarted_thread
    {
        public:
            unstarted_thread(
                uint64_t qts,
                vc::vclock_t vclk,
                void (*f)(void*),
                void *a);

        public:
            bool operator>(const unstarted_thread &t) const;

        public:
            uint64_t qtimestamp;
            vc::vclock_t vclock;
            void (*func)(void*);
            void *arg;
    };

    inline
    unstarted_thread :: unstarted_thread( 
            uint64_t qts,
            vc::vclock_t vclk,
            void (*f)(void*),
            void *a)
        : qtimestamp(qts)
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
            return (r1->qtimestamp) > (r2->qtimestamp);
        }
    };

    // priority queue type definition
    // each shard server has one such priority queue for each vector timestamper
    typedef std::priority_queue<unstarted_thread*, std::vector<unstarted_thread*>, work_thread_compare> pqueue_t;
    
    class pool
    {
        public:
            int num_threads;
            std::vector<pqueue_t> queues;
            vc::qtimestamp_t qts; // queue timestamps
            //std::priority_queue<unstarted_thread*, std::vector<unstarted_thread*>, work_thread_compare> work_queue;
            po6::threads::mutex queue_mutex, thread_loop_mutex;
            po6::threads::cond queue_cond;
            static db::shard *S;
       
        public:
            void add_request(uint64_t vt_id, unstarted_thread*);
            bool check_qts(uint64_t vt_id, uint64_t qts);
            void increment_qts(uint64_t vt_id);
        
        public:
            pool(int n_threads);
    };

    inline
    pool :: pool(int n_threads)
        : num_threads(n_threads)
        , queues(NUM_VTS, pqueue_t())
        , qts(NUM_VTS, 0)
        , queue_cond(&queue_mutex)
    {
        int i;
        std::unique_ptr<std::thread> t;
        for (i = 0; i < num_threads; i++) {
            t.reset(new std::thread(worker_thread_loop, this));
            t->detach();
        }
    }

    inline void
    pool :: add_request(uint64_t vt_id, unstarted_thread *t)
    {
        queue_mutex.lock();
        queue_cond.broadcast();
        queues.at(vt_id).push(t);
        queue_mutex.unlock();
    }

    // check if operation on head of queue corresponding to vt_id
    // is good to go using queue timestamp
    // caution: assuming caller holds queue_mutex
    inline bool
    pool :: check_qts(uint64_t vt_id, uint64_t timestamp)
    {
        return (timestamp <= qts.at(vt_id));
    }

    // increment a queue timestamp after processing request
    inline void
    pool :: increment_qts(uint64_t vt_id)
    {
        queue_mutex.lock();
        qts.at(vt_id)++;
        queue_cond.broadcast();
        queue_mutex.unlock();
    }
} 
}

#endif
