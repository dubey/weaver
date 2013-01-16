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
#include <thread>
#include <po6/threads/mutex.h>
#include <po6/threads/cond.h>

#include "graph.h"
#include "common/message/message.h"

namespace db
{
namespace thread
{
    class pool;
    void thread_loop(pool *tpool);

    class unstarted_thread
    {
        public:
            unstarted_thread(
                void (*f)(db::graph*, std::unique_ptr<message::message>),
                db::graph *g,
                std::unique_ptr<message::message> m);

        public:
            void (*func)(db::graph*, std::unique_ptr<message::message>);
            db::graph *G;
            std::unique_ptr<message::message> msg;
    };

    inline
    unstarted_thread :: unstarted_thread( 
            void (*f)(db::graph*, std::unique_ptr<message::message>),
            db::graph *g,
            std::unique_ptr<message::message> m)
        : func(f)
        , G(g)
        , msg(std::move(m))
    {
    }

    class pool
    {
        public:
            pool(int n_threads);

        public:
            int num_threads;
            std::deque<std::unique_ptr<unstarted_thread>> queue;
            std::vector<std::thread> threads;
            po6::threads::mutex queue_mutex;
            po6::threads::cond empty_queue_cond;
        
        public:
            void add_request(std::unique_ptr<unstarted_thread> t);
    };

    inline
    pool :: pool(int n_threads)
        : num_threads(n_threads)
        , empty_queue_cond(&queue_mutex)
    {
        int i;
        std::unique_ptr<std::thread> t;
        for (i = 0; i < num_threads; i++)
        {
            t.reset(new std::thread(thread_loop, this));
            t->detach();
        }
    }

    inline void
    pool :: add_request(std::unique_ptr<unstarted_thread> t)
    {
        queue_mutex.lock();
        if (queue.empty())
        {
            empty_queue_cond.signal();
        }
        queue.push_back(std::move(t));
        queue_mutex.unlock();
    }

    void
    thread_loop(pool *tpool)
    {
        std::unique_ptr<unstarted_thread> thr;
        while (true)
        {
            tpool->queue_mutex.lock();
            while (tpool->queue.empty())
            {
                tpool->empty_queue_cond.wait();
            }
            thr = std::move(tpool->queue.front());
            tpool->queue.pop_front();
            if (!tpool->queue.empty())
            {
                tpool->empty_queue_cond.signal();
            }
            tpool->queue_mutex.unlock();
            (*(thr->func))(thr->G, std::move(thr->msg));
        }
    }
} //namespace thread
} //namespace db

#endif //__THREADPOOL__
