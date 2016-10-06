/*
 * ===============================================================
 *    Description:  Implementation of shard queue manager.
 *
 *        Created:  2014-02-20 16:53:14
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013-2014, Cornell University, see the LICENSE
 *                     file for licensing agreement
 * ===============================================================
 */

#include <unordered_map>

#define weaver_debug_
#include "common/weaver_constants.h"
#include "common/config_constants.h"
#include "common/event_order.h"
#include "db/queue_manager.h"

using db::queue_order;
using db::queue_manager;
using db::queued_request;

queue_manager :: queue_manager()
    : rd_queues(NumVts, pqueue_t())
    , wr_queues(NumVts, pqueue_t())
    , last_clocks(NumVts, vc::vclock_t(ClkSz, 0))
    , qts(NumVts, 0)
    , min_epoch(NumVts, 0)
{
    last_clocks_ptr.reserve(last_clocks.size());
    for (size_t i = 0; i < last_clocks.size(); i++) {
        last_clocks_ptr.emplace_back(&last_clocks[i]);
    }
}

void
queue_manager :: enqueue_read_request(uint64_t vt_id, queued_request *t)
{
    queue_mutex.lock();
    rd_queues[vt_id].push(t);
    queue_mutex.unlock();
}

// either there is only one gatekeeper and this is the next op from that gk
// or this op's clock is lower than all last executed clocks
// this will return false if clk is after last clock (but not the next op)
bool
queue_manager :: check_rd_request_nonlocking(vc::vclock &clk)
{
    bool ok_to_exec = false;
    // value of last clock from this gatekeeper
    uint64_t gk_last_clk_value = last_clocks[clk.vt_id][clk.vt_id+1];
    // epoch of last clock from this gateekeper
    uint64_t gk_last_clk_epoch = last_clocks[clk.vt_id][0];
    // value of this read request's clock = clk.get_clock() = clk.clock[clk.vt_id+1]
    // epoch of this read_request's clock = clk.get_epoch() = clk.clock[0]
    if (NumVts == 1 &&
        (gk_last_clk_epoch == 0 || gk_last_clk_epoch == clk.get_epoch()) &&
        (gk_last_clk_value + 1) == clk.get_clock()) {
        ok_to_exec = true;
    } else if (check_last_clocks_nonlocking(clk.clock)) {
        ok_to_exec = true;
    }

    if (ok_to_exec) {
        update_last_clocks_nonlocking(clk);
        return true;
    } else {
        return false;
    }
}

// check if the read request received in thread loop can be executed without waiting
bool
queue_manager :: check_rd_request(vc::vclock &clk)
{
    queue_mutex.lock();
    bool check = check_rd_request_nonlocking(clk);
    queue_mutex.unlock();
    return check;
}

void
queue_manager :: enqueue_write_request(uint64_t vt_id, queued_request *t)
{
    queue_mutex.lock();

    if (t->vclock.clock[0] >= min_epoch[vt_id]) {
        wr_queues[vt_id].push(t);
    }

    queue_mutex.unlock();
}

//// true if all queues empty
//bool
//queue_manager :: check_queues_empty()
//{
//    for (uint64_t i = 0; i < NumVts; i++) {
//        pqueue_t &pq = wr_queues[i];
//        if (!pq.empty()) {
//            WDEBUG << "all queues not empty" << std::endl;
//            return false;
//        }
//    }
//
//    return true;
//}

// check if write request received in thread loop can be executed without waiting
// this does not call Kronos, so if vector clocks cannot be compared, the request will be pushed on write queue
enum queue_order
queue_manager :: check_wr_request_nonlocking(vc::vclock &vclk, uint64_t qt)
{
    enum queue_order ret = UNDECIDED;

    // short circuit check
    // this is the next tx from vt_id and vclk is less than all last_clocks
    // no Kronos call
    enum queue_order single_order = check_wr_queue_single(vclk.vt_id, qt);
    if (NumVts == 1) {
        ret = single_order;
    } else if (single_order == PRESENT) {
        if (check_last_clocks_nonlocking(vclk.clock)) {
            ret = PRESENT;
        }
    }

    // if above check did not pan out, then actually compare against
    // tx at the head of each wr queue
    if (ret == UNDECIDED) {
        assert(NumVts > 1);

        enum queue_order cur_order = check_wr_queues_timestamps(vclk.vt_id, qt);
        assert(cur_order != PAST);

        if (cur_order == PRESENT) {
            // all write queues (possibly except vt_id) good to go
            // compare vector clocks, NO Kronos call
            wr_clocks_ptr.resize(NumVts-1, nullptr);
            for (uint64_t i = 0, j = 0; i < NumVts; i++) {
                if (i == vclk.vt_id) {
                    continue;
                } else {
                    wr_clocks_ptr[j++] = &wr_queues[i].top()->vclock.clock;
                }
            }
            if (order::oracle::happens_before_no_kronos(vclk.clock, wr_clocks_ptr)) {
                ret = PRESENT;
            } else {
                ret = FUTURE;
            }
        } else {
            ret = FUTURE;
        }
    }

    assert(ret != UNDECIDED);
    return ret;
}

enum queue_order
queue_manager :: check_wr_request(vc::vclock &vclk, uint64_t qt)
{
    queue_mutex.lock();
    enum queue_order ret = check_wr_request_nonlocking(vclk, qt);
    queue_mutex.unlock();

    return ret;
}

//// check if this is the next expected wr tx from the timestamper
//// if yes, then can execute nop
//bool
//queue_manager :: check_rd_nop(vc::vclock &vclk, uint64_t qt)
//{
//    enum queue_order ret = UNDECIDED;
//
//    queue_mutex.lock();
//    if (check_queues_empty()) {
//        ret = check_wr_queue_single(vclk.vt_id, qt);
//    }
//    queue_mutex.unlock();
//
//    return (ret == PRESENT);
//}
//
//// check if any other queue nonempty
//// then check_wr_req
//bool
//queue_manager :: check_wr_nop(vc::vclock &vclk, uint64_t qt)
//{
//    bool ret = false;
//
//    queue_mutex.lock();
//    if (check_queues_empty()) {
//        ret = true;
//    } else {
//        ret = (PRESENT == check_wr_request_nonlocking(vclk, qt));
//    }
//    queue_mutex.unlock();
//
//    return ret;
//}

// check all read and write queues
// execute a single queued request which can be run now, and return true
// else return false
bool
queue_manager :: exec_queued_request(uint64_t tid, order::oracle *time_oracle)
{
    queue_mutex.lock(); // prevent more jobs from being added
    queued_request *req = get_rw_req();
    queue_mutex.unlock();
    if (req == nullptr) {
        return false;
    }
    req->arg->time_oracle = time_oracle;
    (*req->func)(tid, req->arg);
    // queue timestamp is incremented by the thread, upon enqueueing this tx on node queue
    // when to increment qts depends on the tx components
    delete req;
    return true;
}

// increment queue timestamp for a tx which has been ordered
void
queue_manager :: increment_qts(uint64_t vt_id, uint64_t incr)
{
    queue_mutex.lock();
    qts[vt_id] += incr;
    queue_mutex.unlock();
}

// record the vclk for last completed write tx
void
queue_manager :: record_completed_tx(vc::vclock &tx_clk)
{
    queue_mutex.lock();
    update_last_clocks_nonlocking(tx_clk);
    queue_mutex.unlock();
}

// update last clock of a queue if new clocks happens before last clock
// this happens either after tx has completed execution
// or after node program has been dequeued from a queue
// if update is successful return true
// if new clocks does not happen before last clock return false
bool
queue_manager :: update_last_clocks_nonlocking(vc::vclock &new_clk)
{
    vc::vclock_t &last_clk = last_clocks[new_clk.vt_id];
    if (order::oracle::happens_before_no_kronos(last_clk, new_clk.clock)) {
        last_clk = new_clk.clock;
        return true;
    } else {
        return false;
    }
}

bool
queue_manager :: check_last_clocks_nonlocking(vc::vclock_t &clk)
{
    //return true;
    // no kronos call
    return order::oracle::happens_before_no_kronos(clk, last_clocks_ptr);
}

queued_request*
queue_manager :: get_rd_req()
{
    queued_request *req;
    for (uint64_t vt_id = 0; vt_id < NumVts; vt_id++) {
        pqueue_t &pq = rd_queues[vt_id];
        // execute read request after all write queues have processed write which happens after this read
        if (!pq.empty()) {
            req = pq.top();
            if (check_rd_request_nonlocking(req->vclock)) {
                pq.pop();
                return req;
            }
        }
    }
    return nullptr;
}

// check that "qt" is the next write expected from "vt_id"
enum queue_order
queue_manager :: check_wr_queue_single(uint64_t vt_id, uint64_t qt)
{
    assert(qt > qts[vt_id]);
    if (qt > (qts[vt_id]+1)) {
        return FUTURE;
    } else {
        return PRESENT;
    }
}

// true if all queues either empty or have nop at head
// return indices of nops
bool
queue_manager :: check_all_nops(std::vector<uint64_t> &nops_idx)
{
    uint64_t num_empty = 0;

    for (uint64_t i = 0; i < NumVts; i++) {
        pqueue_t &pq = wr_queues[i];
        if (pq.empty()) {
            num_empty++;
        } else {
            if (pq.top()->type == NOP
             && (qts[i] + 1)   == pq.top()->priority) {
                nops_idx.emplace_back(i);
            } else {
                return false;
            }
        }
    }

    return (num_empty != NumVts);
}

enum queue_order
queue_manager :: check_wr_queues_timestamps(uint64_t vt_id, uint64_t qt)
{
    // check each write queue ready to go
    for (uint64_t i = 0; i < NumVts; i++) {
        if (vt_id == i) {
            assert(qt > qts[i]);
            if (qt > (qts[i]+1)) {
                return FUTURE;
            }
        } else {
            pqueue_t &pq = wr_queues[i];
            if (pq.empty()) { // can't go on if one of the pq's is empty
                return FUTURE;
            } else {
                // check for correct ordering of queue timestamp (which is priority for thread)
                if ((qts[i] + 1) != pq.top()->priority) {
                    return FUTURE;
                }
            }
        }
    }
    return PRESENT;
}

queued_request*
queue_manager :: get_wr_req()
{
    uint64_t exec_vt_id;

    std::vector<uint64_t> nops_idx;
    check_all_nops(nops_idx);
    if (check_all_nops(nops_idx)) {
        uint64_t choose_idx = weaver_util::urandom_uint64() % nops_idx.size();
        exec_vt_id = nops_idx[choose_idx];
    } else {
        enum queue_order queue_status = check_wr_queues_timestamps(UINT64_MAX, UINT64_MAX);
        assert(queue_status != PAST);
        if (queue_status == FUTURE) {
            return nullptr;
        }

        // all write queues are good to go
        if (NumVts == 1) {
            exec_vt_id = 0; // only one timestamper
        } else {
            // compare timestamps, may call Kronos
            std::vector<vc::vclock> timestamps;
            timestamps.reserve(NumVts);
            for (uint64_t vt_id = 0; vt_id < NumVts; vt_id++) {
                timestamps.emplace_back(wr_queues[vt_id].top()->vclock);
                assert(timestamps.back().clock.size() == ClkSz);
            }
            exec_vt_id = time_oracle.compare_vts(timestamps);
        }
    }

    queued_request *req = wr_queues[exec_vt_id].top();
    wr_queues[exec_vt_id].pop();
    return req;
}


queued_request*
queue_manager :: get_rw_req()
{
    queued_request *req = get_rd_req();
    if (req == nullptr) {
        req = get_wr_req();
    }
    return req;
}

void
queue_manager :: reset(uint64_t dead_vt, uint64_t new_epoch)
{
    queue_mutex.lock();

    assert(new_epoch > min_epoch[dead_vt]);
    min_epoch[dead_vt] = new_epoch;

    qts[dead_vt] = 0;
    last_clocks[dead_vt] = vc::vclock_t(ClkSz, 0);
    last_clocks_ptr[dead_vt] = &last_clocks[dead_vt];

    pqueue_t &dead_queue = wr_queues[dead_vt];
    while (!dead_queue.empty()
        && dead_queue.top()->vclock.clock[0] < min_epoch[dead_vt]) {
        dead_queue.pop();
    }

    queue_mutex.unlock();
}

void
queue_manager :: clear_queued_reads()
{
    rd_queues = std::vector<pqueue_t>(NumVts, pqueue_t());
}
