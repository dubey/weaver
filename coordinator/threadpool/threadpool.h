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
#include <po6/net/location.h>
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
    void thread_loop(pool *tpool, bool client);

    class unstarted_thread
    {
        public:
            unstarted_thread(
                void (*f)(coordinator::central*, std::unique_ptr<message::message>, enum message::msg_type, std::unique_ptr<po6::net::location>),
                coordinator::central *s,
                std::unique_ptr<message::message> m,
                enum message::msg_type mtype,
                std::unique_ptr<po6::net::location> l);

        public:
            void (*func)(coordinator::central*,
                         std::unique_ptr<message::message>, 
                         enum message::msg_type,
                         std::unique_ptr<po6::net::location>);
            coordinator::central *server;
            std::unique_ptr<message::message> msg;
            enum message::msg_type m_type;
            std::unique_ptr<po6::net::location> loc;
    };

    inline
    unstarted_thread :: unstarted_thread( 
            void (*f)(coordinator::central*, 
                      std::unique_ptr<message::message>,
                      enum message::msg_type,
                      std::unique_ptr<po6::net::location>),
            coordinator::central *s,
            std::unique_ptr<message::message> m,
            enum message::msg_type mtype,
            std::unique_ptr<po6::net::location> l)
        : func(f)
        , server(s)
        , msg(std::move(m))
        , m_type(mtype)
        , loc(std::move(l))
    {
    }

    class pool
    {
        public:
            pool(int n_threads);

        public:
            int num_threads;
            std::deque<std::unique_ptr<unstarted_thread>> client_req_queue;
            std::deque<std::unique_ptr<unstarted_thread>> shard_response_queue;
            std::vector<std::thread> threads;
            po6::threads::mutex queue_mutex;
            po6::threads::cond shard_queue_cond;
            po6::threads::cond client_queue_cond;
        
        public:
            void add_request(std::unique_ptr<unstarted_thread> t, bool client);
    };

    inline
    pool :: pool(int n_threads)
        : num_threads(n_threads)
        , shard_queue_cond(&queue_mutex)
        , client_queue_cond(&queue_mutex)
    {
        int i;
        std::unique_ptr<std::thread> t;
        for (i = 0; i < num_threads; i++) {
            t.reset(new std::thread(thread_loop, this, false));
            t->detach();
        }
        for (i = 0; i < num_threads; i++) {
            t.reset(new std::thread(thread_loop, this, true));
            t->detach();
        }
    }

    inline void
    pool :: add_request(std::unique_ptr<unstarted_thread> t, bool client)
    {
        std::unique_ptr<std::thread> thr;
        queue_mutex.lock();
        if (client) {
            if (client_req_queue.empty()) {
                client_queue_cond.signal();
            }
            client_req_queue.push_back(std::move(t));
        } else {
            if (shard_response_queue.empty()) {
                shard_queue_cond.signal();
            }
            shard_response_queue.push_back(std::move(t));
        }
        queue_mutex.unlock();
    }

    void
    thread_loop(pool *tpool, bool client)
    {
        std::unique_ptr<unstarted_thread> thr;
        while (true) {
            tpool->queue_mutex.lock();
            if (client) {
                while(tpool->client_req_queue.empty()) {
                    tpool->client_queue_cond.wait();
                }
                thr = std::move(tpool->client_req_queue.front());
                tpool->client_req_queue.pop_front();
                if (!tpool->client_req_queue.empty()) {
                    tpool->client_queue_cond.signal();
                }
            } else {
                while(tpool->shard_response_queue.empty()) {
                    tpool->shard_queue_cond.wait();
                }
                thr = std::move(tpool->shard_response_queue.front());
                tpool->shard_response_queue.pop_front();
                if (!tpool->shard_response_queue.empty()) {
                    tpool->shard_queue_cond.signal();
                }
            }
            tpool->queue_mutex.unlock();
            (*(thr->func))(thr->server, std::move(thr->msg), thr->m_type, std::move(thr->loc));
        }
    }
} 
} 

#endif //__CS_THREADPOOL__
