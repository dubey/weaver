/*
 * ===============================================================
 *    Description:  Thread pool for all servers except central
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
class graph;

namespace thread
{
    class pool;
    void worker_thread_loop(pool *tpool);

    class unstarted_thread
    {
        public:
            unstarted_thread(
                uint64_t s,
                void (*f)(void*),
                void *a);

        public:
            bool operator>(const unstarted_thread &t) const;

        public:
            uint64_t start_time;
            void (*func)(void*);
            void *arg;
    };

    inline
    unstarted_thread :: unstarted_thread( 
            uint64_t s,
            void (*f)(void*),
            void *a)
        : start_time(s)
        , func(f)
        , arg(a)
    {
    }

    // for priority_queue
    struct work_thread_compare 
        : std::binary_function<unstarted_thread*, unstarted_thread*, bool>
    {
        bool operator()(const unstarted_thread* const &r1, const unstarted_thread* const &r2)
        {
            return (r1->start_time) > (r2->start_time);
        }
    };
    
    class pool
    {
        public:
            int num_threads;
            std::priority_queue<unstarted_thread*, std::vector<unstarted_thread*>, work_thread_compare> work_queue;
            po6::threads::mutex queue_mutex;
            po6::threads::cond work_queue_cond;
            static db::graph *G;
       
        public:
            void add_request(unstarted_thread*);
        
        public:
            pool(int n_threads);
    };

    inline
    pool :: pool(int n_threads)
        : num_threads(n_threads)
        , work_queue_cond(&queue_mutex)
    {
        int i;
        std::unique_ptr<std::thread> t;
        for (i = 0; i < num_threads; i++) {
            t.reset(new std::thread(worker_thread_loop, this));
            t->detach();
        }
    }

    inline void
    pool :: add_request(unstarted_thread *t)
    {
        queue_mutex.lock();
        work_queue_cond.broadcast();
        work_queue.push(t);
        queue_mutex.unlock();
    }
} 
}

#endif
