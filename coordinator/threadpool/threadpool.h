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

#include "common/message.h"
#include "common/weaver_constants.h"


namespace coordinator
{

class central;

namespace thread
{
    class pool;
    void thread_loop(pool *tpool);

    class unstarted_thread
    {
        public:
            unstarted_thread(
                void (*f)(coordinator::central*, std::unique_ptr<message::message>, enum message::msg_type, uint64_t),
                coordinator::central *s,
                std::unique_ptr<message::message> m,
                enum message::msg_type mtype,
                uint64_t l);

        public:
            void (*func)(coordinator::central*,
                         std::unique_ptr<message::message>, 
                         enum message::msg_type,
                         uint64_t);
            coordinator::central *server;
            std::unique_ptr<message::message> msg;
            enum message::msg_type m_type;
            uint64_t loc;
    };

    inline
    unstarted_thread :: unstarted_thread( 
            void (*f)(coordinator::central*, 
                      std::unique_ptr<message::message>,
                      enum message::msg_type,
                      uint64_t),
            coordinator::central *s,
            std::unique_ptr<message::message> m,
            enum message::msg_type mtype,
            uint64_t l)
        : func(f)
        , server(s)
        , msg(std::move(m))
        , m_type(mtype)
        , loc(l)
    { }

    class pool
    {
        public:
            pool(int n_threads);

        public:
            int num_threads;
            std::deque<std::unique_ptr<unstarted_thread>> work_queue;
            std::vector<std::thread> threads;
            po6::threads::mutex queue_mutex;
            po6::threads::cond queue_cond;
        
        public:
            void add_request(std::unique_ptr<unstarted_thread> t);
    };

    inline
    pool :: pool(int n_threads)
        : num_threads(n_threads)
        , queue_cond(&queue_mutex)
    {
        int i;
        std::unique_ptr<std::thread> t;
        for (i = 0; i < num_threads; i++) {
            t.reset(new std::thread(thread_loop, this));
            t->detach();
        }
    }

    inline void
    pool :: add_request(std::unique_ptr<unstarted_thread> t)
    {
        std::unique_ptr<std::thread> thr;
        queue_mutex.lock();
        if (work_queue.empty()) {
            queue_cond.signal();
        }
        work_queue.push_back(std::move(t));
        queue_mutex.unlock();
    }

    void
    thread_loop(pool *tpool)
    {
        std::unique_ptr<unstarted_thread> thr;
        while (true) {
            tpool->queue_mutex.lock();
            while(tpool->work_queue.empty()) {
                tpool->queue_cond.wait();
            }
            thr = std::move(tpool->work_queue.front());
            tpool->work_queue.pop_front();
            if (!tpool->work_queue.empty()) {
                tpool->queue_cond.signal();
            }
            tpool->queue_mutex.unlock();
            (*(thr->func))(thr->server, std::move(thr->msg), thr->m_type, std::move(thr->loc));
        }
    }
} 
} 

#endif
