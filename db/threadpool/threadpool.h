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
#include "common/message.h"

namespace db
{
class graph;

namespace thread
{
    class pool;
    void thread_loop(pool *tpool, bool update_thread);

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
            int num_threads;
            std::deque<std::unique_ptr<unstarted_thread>> reach_req_queue;
            std::deque<std::unique_ptr<unstarted_thread>> update_queue;
            po6::threads::mutex queue_mutex;
            po6::threads::cond empty_queue_cond;
            po6::threads::cond graph_update_queue_cond;
            int num_free_update, num_free_reach;
       
        public:
            void add_request(std::unique_ptr<unstarted_thread>, bool);
        
        public:
            pool(int n_threads);
    };

    inline
    pool :: pool(int n_threads)
        : num_threads(n_threads)
        , empty_queue_cond(&queue_mutex)
        , graph_update_queue_cond(&queue_mutex)
        , num_free_update(0)
        , num_free_reach(0)
    {
        int i;
        std::unique_ptr<std::thread> t;
        for (i = 0; i < num_threads; i++) // reachability requests
        {
            t.reset(new std::thread(thread_loop, this, false));
            t->detach();
        }
        for (i = 0; i < num_threads; i++) // graph updates
        {
            t.reset(new std::thread(thread_loop, this, true));
            t->detach();
        }
    }

    inline void
    pool :: add_request(std::unique_ptr<unstarted_thread> t, bool update_req = false)
    {
        std::unique_ptr<std::thread> thr;
        queue_mutex.lock();
        if (update_req)
        {
            if (num_free_update == num_threads)
            {
                // need to create a new thread
                // since all other threads may be waiting for an update
                thr.reset(new std::thread(t->func, t->G, std::move(t->msg)));
                thr->detach();
            } else
            { 
                if (update_queue.empty()) {
                    graph_update_queue_cond.signal();
                }
                update_queue.push_back(std::move(t));
            }
        } else {
            if (reach_req_queue.empty()) {
                empty_queue_cond.signal();
            }
            reach_req_queue.push_back(std::move(t));
        }
        queue_mutex.unlock();
    }

    void
    thread_loop(pool *tpool, bool update_thread = false)
    {
        std::unique_ptr<unstarted_thread> thr;
        while (true)
        {
            tpool->queue_mutex.lock();
            if (update_thread)
            {
                tpool->num_free_update++;
                while (tpool->update_queue.empty())
                {
                    tpool->graph_update_queue_cond.wait();
                }
                tpool->num_free_update--;
                thr = std::move(tpool->update_queue.front());
                tpool->update_queue.pop_front();
                if (!tpool->update_queue.empty())
                {
                    tpool->graph_update_queue_cond.signal();
                }
            } else {
                tpool->num_free_reach++;
                while (tpool->reach_req_queue.empty())
                {
                    tpool->empty_queue_cond.wait();
                }
                tpool->num_free_reach--;
                thr = std::move(tpool->reach_req_queue.front());
                tpool->reach_req_queue.pop_front();
                if (!tpool->reach_req_queue.empty())
                {
                    tpool->empty_queue_cond.signal();
                }
            }
            tpool->queue_mutex.unlock();
            (*(thr->func))(thr->G, std::move(thr->msg));
        }
    }
} 
}

#endif //__THREADPOOL__
