/*
 * ===============================================================
 *    Description:  Thread pool for coordinator
 *
 *        Created:  01/09/2013 04:17:11 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __CS_THREADPOOL__
#define __CS_THREADPOOL__

#include <vector>
#include <deque>
#include <thread>
#include <po6/threads/mutex.h>
#include <po6/threads/cond.h>

#include "central.h"
#include "common/message/message.h"

namespace coordinator
{
namespace thread
{
    class pool;
    void thread_loop(pool *tpool, bool update);

    class unstarted_thread
    {
        public:
            unstarted_thread(
                void (*f)(coordinator::central*, std::unique_ptr<message::message>, enum message::msg_type),
                coordinator::central *s,
                std::unique_ptr<message::message> m,
                enum message::msg_type mtype);

        public:
            void (*func)(coordinator::central*,
                         std::unique_ptr<message::message>, 
                         enum message::msg_type);
            coordinator::central *server;
            std::unique_ptr<message::message> msg;
            enum message::msg_type m_type;
    };

    inline
    unstarted_thread :: unstarted_thread( 
            void (*f)(coordinator::central*, 
                      std::unique_ptr<message::message>,
                      enum message::msg_type),
            coordinator::central *s,
            std::unique_ptr<message::message> m,
            enum message::msg_type mtype)
        : func(f)
        , server(s)
        , msg(std::move(m))
        , m_type(mtype)
    {
    }

    class pool
    {
        public:
            pool(int n_threads);

        public:
            int num_threads;
            std::deque<std::unique_ptr<unstarted_thread>> update_queue;
            std::deque<std::unique_ptr<unstarted_thread>> reach_req_queue;
            std::vector<std::thread> threads;
            po6::threads::mutex queue_mutex;
            po6::threads::cond empty_queue_cond;
            po6::threads::cond update_empty_queue_cond;
        
        public:
            void add_request(std::unique_ptr<unstarted_thread> t, bool update);
    };

    inline
    pool :: pool(int n_threads)
        : num_threads(n_threads)
        , empty_queue_cond(&queue_mutex)
        , update_empty_queue_cond(&queue_mutex)
    {
        int i;
        std::unique_ptr<std::thread> t;
        for (i = 0; i < num_threads; i++)
        {
            t.reset(new std::thread(thread_loop, this, false));
            t->detach();
        }
        t.reset(new std::thread(thread_loop, this, true));
        t->detach();
    }

    inline void
    pool :: add_request(std::unique_ptr<unstarted_thread> t, bool update)
    {
        queue_mutex.lock();
        if (update)
        {
            if (update_queue.empty())
            {
                update_empty_queue_cond.signal();
            }
            update_queue.push_back(std::move(t));
        } else {
            if (reach_req_queue.empty())
            {
                empty_queue_cond.signal();
            }
            reach_req_queue.push_back(std::move(t));
        }
        queue_mutex.unlock();
    }

    void
    thread_loop(pool *tpool, bool update)
    {
        std::unique_ptr<unstarted_thread> thr;
        while (true)
        {
            tpool->queue_mutex.lock();
            if (update) {
                while(tpool->update_queue.empty())
                {
                    tpool->update_empty_queue_cond.wait();
                }
                thr = std::move(tpool->update_queue.front());
                tpool->update_queue.pop_front();
            } else {
                while(tpool->reach_req_queue.empty())
                {
                    tpool->empty_queue_cond.wait();
                }
                thr = std::move(tpool->reach_req_queue.front());
                tpool->reach_req_queue.pop_front();
                if (!tpool->reach_req_queue.empty())
                {
                    tpool->empty_queue_cond.signal();
                }
            }
            tpool->queue_mutex.unlock();
            (*(thr->func))(thr->server, std::move(thr->msg), thr->m_type);
        }
    }
} //namespace thread
} //namespace db

#endif //__CS_THREADPOOL__
