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

#include "graph.h"
#include "common/message.h"

namespace db
{
class graph;
class batch_request;

namespace thread
{
    class pool;
    void thread_loop(pool *tpool);
    void traversal_thread_loop(pool *tpool);

    class unstarted_traversal_thread
    {
        public:
            unstarted_traversal_thread(
                void (*f)(graph*, batch_request*),
                graph *g,
                batch_request *r);

        public:
            bool operator>(const unstarted_traversal_thread &t) const;

        public:
            void (*func)(graph*, batch_request*);
            graph *G;
            batch_request *req;
    };

    inline
    unstarted_traversal_thread :: unstarted_traversal_thread( 
            void (*f)(graph*, batch_request*),
            graph *g,
            batch_request *r)
        : func(f)
        , G(g)
        , req(r)
    {
    }

    // for priority_queue
    struct traversal_req_compare 
        : std::binary_function<unstarted_traversal_thread*, unstarted_traversal_thread*, bool>
    {
        bool operator()(const unstarted_traversal_thread* const &r1, const unstarted_traversal_thread* const&r2)
        {
            return (*r1 > *r2);
        }
    };
    
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
            std::priority_queue<unstarted_traversal_thread*, std::vector<unstarted_traversal_thread*>, traversal_req_compare> traversal_queue;
            std::deque<std::unique_ptr<unstarted_thread>> update_queue;
            po6::threads::mutex queue_mutex;
            po6::threads::cond empty_queue_cond;
            po6::threads::cond graph_update_queue_cond;
            int num_free_update, num_free_reach;
       
        public:
            void add_request(std::unique_ptr<unstarted_thread>);
            void add_traversal_request(unstarted_traversal_thread*);
        
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
            t.reset(new std::thread(traversal_thread_loop, this));
            t->detach();
        }
        for (i = 0; i < num_threads; i++) // graph updates
        {
            t.reset(new std::thread(thread_loop, this));
            t->detach();
        }
    }

    inline void
    pool :: add_request(std::unique_ptr<unstarted_thread> t)
    {
        std::unique_ptr<std::thread> thr;
        queue_mutex.lock();
        if (num_free_update == 0)
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
        queue_mutex.unlock();
    }

    inline void
    pool :: add_traversal_request(unstarted_traversal_thread *t)
    {
        queue_mutex.lock();
        if (traversal_queue.empty()) {
            empty_queue_cond.signal();
        }
        traversal_queue.push(t);
        queue_mutex.unlock();
    }

} 
}

#endif //__THREADPOOL__
