/*
 * ===============================================================
 *    Description:  Shard queues for storing requests which cannot
 *                  be executed on receipt due to ordering
 *                  constraints.
 *
 *        Created:  2014-02-20 16:41:22
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013-2014, Cornell University, see the LICENSE
 *                     file for licensing agreement
 * ===============================================================
 */

#ifndef weaver_db_queue_manager_h_
#define weaver_db_queue_manager_h_

#include <queue>
#include <thread>
#include <po6/threads/mutex.h>

#include "db/queued_request.h"

namespace db
{
    enum queue_order
    {
        PAST,
        PRESENT,
        FUTURE
    };

    // priority queue type definition
    // each shard server has one such priority queue for each vector timestamper
    typedef std::priority_queue<queued_request*, std::vector<queued_request*>, work_thread_compare> pqueue_t;

    class queue_manager
    {
        private:
            std::vector<pqueue_t> rd_queues;
            std::vector<pqueue_t> wr_queues;
            std::vector<vc::vclock_t> last_clocks; // records last transaction vclock pulled of queue for each vector timestamper
            vc::qtimestamp_t qts; // queue timestamps
            po6::threads::mutex queue_mutex;

        private:
            queued_request* get_rd_req();
            queued_request* get_wr_req();
            queued_request* get_rw_req();
            bool check_rd_req_nonlocking(vc::vclock_t &clk);
            enum queue_order check_wr_queues_timestamps(uint64_t vt_id, uint64_t qt);

        public:
            queue_manager();
            void enqueue_read_request(uint64_t vt_id, queued_request*);
            bool check_rd_request(vc::vclock_t &clk);
            void enqueue_write_request(uint64_t vt_id, queued_request*);
            enum queue_order check_wr_request(vc::vclock &vclk, uint64_t qt);
            bool exec_queued_request();
            void increment_qts(uint64_t vt_id, uint64_t incr);
            void record_completed_tx(vc::vclock &tx_clk);
            // fault tolerance
            void restore_backup(std::unordered_map<uint64_t, uint64_t> &qts);
            void set_qts(uint64_t vt_id, uint64_t rec_qts);
    };

}

#endif
