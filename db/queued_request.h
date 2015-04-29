/*
 * ===============================================================
 *    Description:  Data structure to hold a request (write tx or
 *                  node program) which cannot be executed on
 *                  receipt due to ordering constraints.
 *
 *        Created:  2014-02-20 16:35:42
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013-2014, Cornell University, see the LICENSE
 *                     file for licensing agreement
 * ===============================================================
 */

#ifndef weaver_db_queued_request_h_
#define weaver_db_queued_request_h_

#include "common/vclock.h"
#include "db/message_wrapper.h"

namespace db
{
    class queued_request
    {
        public:
            queued_request(uint64_t prio, vc::vclock vclk, void (*f)(uint64_t, message_wrapper*), message_wrapper *a)
                : priority(prio)
                , vclock(vclk)
                , func(f)
                , arg(a)
            { }

        public:
            uint64_t priority;
            vc::vclock vclock;
            void (*func)(uint64_t, message_wrapper*);
            message_wrapper *arg;
    };

    // for work queues
    struct work_thread_compare 
        : std::binary_function<queued_request*, queued_request*, bool>
    {
        bool operator()(const queued_request* const &r1, const queued_request* const &r2)
        {
            return (r1->priority) > (r2->priority);
        }
    };

}

#endif
