/*
 * ===============================================================
 *    Description:  Vector timestamper server loop and request
 *                  processing methods.
 *
 *        Created:  07/22/2013 02:42:28 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <iostream>
#include <thread>
#include <vector>
#include <deque>
#include <stdlib.h>
#include <signal.h>
#include <sys/time.h>

#define weaver_debug_
//#define weaver_test_
#include "common/vclock.h"
#include "common/transaction.h"
#include "common/event_order.h"
#include "common/config_constants.h"
#include "node_prog/node_prog_type.h"
#include "node_prog/node_program.h"
#include "coordinator/vt_constants.h"
#include "coordinator/timestamper.h"

using coordinator::current_prog;
using coordinator::current_tx;
static coordinator::timestamper *vts;
static uint64_t vt_id;

#ifdef weaver_test_
static int num_prep, num_comm;
static po6::threads::mutex tx_count_mtx;
#endif

// tx functions
bool
unpack_tx(message::message &msg, uint64_t client_id, transaction::pending_tx &tx);
void send_abort(uint64_t cl_id);
bool lock_del_elems(std::unordered_set<uint64_t> &del_elems);
void unlock_del_elems(std::unordered_set<uint64_t> &del_elems);
void prepare_tx_step1(std::unique_ptr<transaction::pending_tx> tx,
    nmap::nmap_stub *nmstub,
    coordinator::hyper_stub *hstub);
void release_dist_del_locks(std::bitset<NUM_VTS> &locks,
    std::unordered_set<uint64_t> &del_elems);
void release_locks_and_abort(uint64_t tx_id, coordinator::hyper_stub *hstub);
void done_prepare_tx_step1(uint64_t tx_id,
    uint64_t from_vt,
    nmap::nmap_stub *nmstub,
    coordinator::hyper_stub *hstub);
void fail_prepare_tx_step1(uint64_t tx_id, coordinator::hyper_stub *hstub);
void unbusy_elems(std::vector<uint64_t> &busy_elems);
void prepare_tx_step2(std::unique_ptr<transaction::pending_tx> tx,
    nmap::nmap_stub *nmstub,
    coordinator::hyper_stub *hstub);
void end_transaction(uint64_t tx_id, coordinator::hyper_stub *hstub);


// SIGINT handler
void
end_program(int signum)
{
    std::cerr << "Ending program, signum = " << signum << std::endl;
    vts->clk_rw_mtx.wrlock();
    WDEBUG << "num vclk updates " << vts->clk_updates << std::endl;
    vts->clk_rw_mtx.unlock();
    if (signum == SIGINT) {
        vts->exit_mutex.lock();
        vts->to_exit = true;
        vts->exit_mutex.unlock();
    } else {
        WDEBUG << "Got interrupt signal other than SIGINT, exiting immediately." << std::endl;
        exit(0);
    }
}

// unpack transaction and return list of writes and delete-affected nodes
bool
unpack_tx(message::message &msg,
    uint64_t client_id,
    transaction::pending_tx &tx)
{
    msg.unpack_client_tx(tx);
    tx.id = vts->generate_id();
    tx.client_id = client_id;

    for (auto upd: tx.writes) {
        if (upd->type == transaction::NODE_DELETE_REQ || upd->type == transaction::EDGE_DELETE_REQ) {
            // for delete_node, lock node
            // for delete_edge, lock edge
            if (tx.del_elems.find(upd->elem1) != tx.del_elems.end()) {
                return false;
            }
            tx.del_elems.emplace(upd->elem1);
        }
    }

    return true;
}

// send an abort msg to the waiting client
// all elems should be unlocked, no residual tx state after this
void
send_abort(uint64_t cl_id)
{
    message::message msg;
    msg.prepare_message(message::CLIENT_TX_ABORT);
    vts->comm.send(cl_id, msg.buf);
}

// caution: assuming caller holds vts->busy_mtx
bool
lock_del_elems(uint64_t tx_id, std::unordered_set<uint64_t> &del_elems)
{
    bool fail = false;
    std::vector<uint64_t> added;
    added.reserve(del_elems.size());
    for (uint64_t d: del_elems) {
        if (vts->deleted_elems.find(d) != vts->deleted_elems.end()) {
            if (vts->deleted_elems[d] != tx_id) {
                fail = true;
                break;
            }
        } else if (vts->busy_elems.find(d) != vts->busy_elems.end()) {
            fail = true;
            break;
        } else {
            vts->deleted_elems[d] = tx_id;
            added.emplace_back(d);
        }
    }

    if (fail) {
        // one of the del_elems locked by a concurrent tx
        // undo all locks by this transaction and abort
        for (uint64_t a: added) {
            vts->deleted_elems.erase(a);
        }
        return false;
    }

    return true;
}

// caution: assuming caller holds vts->busy_mtx
void
unlock_del_elems(std::unordered_set<uint64_t> &del_elems)
{
    for (uint64_t d: del_elems) {
        assert(vts->deleted_elems.find(d) != vts->deleted_elems.end());
        vts->deleted_elems.erase(d);
    }
}

void
prepare_tx_step1(std::unique_ptr<transaction::pending_tx> tx,
    nmap::nmap_stub *nmstub,
    coordinator::hyper_stub *hstub)
{
#ifdef weaver_test_
    tx_count_mtx.lock();
    num_prep++;
    WDEBUG << "num prep " << num_prep << ", num outst " << num_comm << std::endl;
    tx_count_mtx.unlock();
#endif

    hstub->prepare_tx(*tx);
    bool fail = false;
    std::unordered_set<uint64_t> del_elems;
    bool dist_lock = (tx->del_elems.size() > 0) && (NUM_VTS > 1);
    uint64_t tx_id = tx->id;
    uint64_t cl_id = tx->client_id;
    if (dist_lock) {
        del_elems = tx->del_elems;
    }

    vts->busy_mtx.lock();

    if (!lock_del_elems(tx->id, tx->del_elems)) {
        fail = true;
    }

    if (!fail && dist_lock) {
        vts->del_tx.emplace(tx_id, current_tx(std::move(tx)));
        vts->del_tx[tx_id].locks.set(vt_id);
        vts->del_tx[tx_id].count = NUM_VTS-1; // dist_lock ensures count > 0
    }

    vts->busy_mtx.unlock();

    if (fail) {
#ifdef weaver_test_
        tx_count_mtx.lock();
        num_prep--;
        WDEBUG << "num prep " << num_prep << ", num outst " << num_comm << std::endl;
        tx_count_mtx.unlock();
#endif
        hstub->del_tx(tx->id);
        send_abort(cl_id);
    } else if (dist_lock) {
        // successfully locked all del_elems on this shard
        // now send lock request to other shards
        for (uint64_t i = 0; i < NUM_VTS; i++) {
            if (i != vt_id) {
                message::message msg;
                msg.prepare_message(message::PREP_DEL_TX, vt_id, tx_id, del_elems);
                vts->comm.send(i, msg.buf);
            }
        }
    } else {
        // no distributed locks, can go ahead with next step
        prepare_tx_step2(std::move(tx), nmstub, hstub);
    }
}

// caution: assuming caller holds vts->busy_mtx
void
release_dist_del_locks(std::bitset<NUM_VTS> &locks,
    std::unordered_set<uint64_t> &del_elems)
{
    message::message msg;
    for (uint64_t i = 0; i < NUM_VTS; i++) {
        if (i == vt_id) {
            assert(locks[i]);
            unlock_del_elems(del_elems);
        } else {
            if (locks[i]) {
                msg.prepare_message(message::UNPREP_DEL_TX, del_elems);
                vts->comm.send(i, msg.buf);
            }
        }
    }
}

// caution: assuming caller holds vts->busy_mtx
void
release_locks_and_abort(uint64_t tx_id, coordinator::hyper_stub *hstub)
{
    auto &cur_tx = vts->del_tx[tx_id];
    uint64_t cl_id = cur_tx.tx->client_id;
    bool dist_lock = (cur_tx.tx->del_elems.size() > 0) && (NUM_VTS > 1);

    if (dist_lock) {
        release_dist_del_locks(cur_tx.locks, cur_tx.tx->del_elems);
    }

    vts->del_tx.erase(tx_id);
#ifdef weaver_test_
    tx_count_mtx.lock();
    num_prep--;
    WDEBUG << "num prep " << num_prep << ", num outst " << num_comm << std::endl;
    tx_count_mtx.unlock();
#endif
    hstub->del_tx(tx_id);

    send_abort(cl_id);
}

void
done_prepare_tx_step1(uint64_t tx_id,
    uint64_t from_vt,
    nmap::nmap_stub *nmstub,
    coordinator::hyper_stub *hstub)
{
    vts->busy_mtx.lock();
    assert(vts->del_tx.find(tx_id) != vts->del_tx.end());
    auto &cur_tx = vts->del_tx[tx_id];

    assert(--cur_tx.count >= 0);
    cur_tx.locks.set(from_vt);

    if (cur_tx.locks.all()) {
        // ready to run
        auto tx = std::move(cur_tx.tx);
        vts->del_tx.erase(tx_id);
        vts->busy_mtx.unlock();

        prepare_tx_step2(std::move(tx), nmstub, hstub);
    } else if (cur_tx.count == 0) {
        // all replies received
        // acquire locks failed on some vts, abort tx
        release_locks_and_abort(tx_id, hstub);
        vts->busy_mtx.unlock();
    } else {
        // some replies pending
        vts->busy_mtx.unlock();
    }
}

void
fail_prepare_tx_step1(uint64_t tx_id, coordinator::hyper_stub *hstub)
{
    vts->busy_mtx.lock();
    assert(vts->del_tx.find(tx_id) != vts->del_tx.end());
    auto &cur_tx = vts->del_tx[tx_id];

    assert(--cur_tx.count >= 0);

    if (cur_tx.count == 0) {
        // some lock acquires failed, release locks and fail tx
        release_locks_and_abort(tx_id, hstub);
    }

    vts->busy_mtx.unlock();
}

// caution: assuming caller holds vts->busy_mtx
void
unbusy_elems(std::vector<uint64_t> &busy_all)
{
    auto &busy_elems = vts->busy_elems;
    for (uint64_t e: busy_all) {
        assert(busy_elems.find(e) != busy_elems.end());
        if (--busy_elems[e] == 0) {
            busy_elems.erase(e);
        }
    }
}

// input: list of writes that are part of this transaction
// for all delete writes, distributed locks on node/edge have been acquired
// acquire local locks for all other graph elems in this transaction (can fail)
// perform node map lookups for all graph elems (can fail)
// remove node mappings for delete_nodes (can fail)
// if all previous succeeds, send out transaction components to shards
void
prepare_tx_step2(std::unique_ptr<transaction::pending_tx> tx,
    nmap::nmap_stub *nmstub,
    coordinator::hyper_stub *hstub)
{
    std::unordered_map<uint64_t, uint64_t> put_map;
    std::unordered_set<uint64_t> get_set;
    std::unordered_set<uint64_t> del_set;
    tx->busy_elems.reserve(tx->writes.size());
    uint64_t busy_single[3];
    busy_single[0] = UINT64_MAX;
    busy_single[1] = UINT64_MAX;
    busy_single[2] = UINT64_MAX;

    vts->busy_mtx.lock();

    bool success = true;
    auto &deleted_elems = vts->deleted_elems;
    auto &busy_elems = vts->busy_elems;
    assert(vts->del_tx.find(tx->id) == vts->del_tx.end());

    for (auto upd: tx->writes) {
        switch (upd->type) {

            case transaction::NODE_CREATE_REQ:
                // randomly assign shard for this node
                vts->loc_gen_mutex.lock();
                vts->loc_gen = (vts->loc_gen + 1) % NUM_SHARDS;
                upd->loc1 = vts->loc_gen + SHARD_ID_INCR; // node will be placed on this shard
                vts->loc_gen_mutex.unlock();
                put_map.emplace(upd->id, upd->loc1);

                assert(deleted_elems.find(upd->id) == deleted_elems.end());
                assert(busy_elems.find(upd->id) == busy_elems.end());
                busy_single[0] = upd->id;
                break;

            case transaction::EDGE_CREATE_REQ:
                if (put_map.find(upd->elem1) == put_map.end()) {
                    get_set.insert(upd->elem1);
                }
                if (put_map.find(upd->elem2) == put_map.end()) {
                    get_set.insert(upd->elem2);
                }

                busy_single[0] = upd->id;
                busy_single[1] = upd->elem1;
                busy_single[2] = upd->elem2;
                break;

            case transaction::NODE_DELETE_REQ:
            case transaction::NODE_SET_PROPERTY:
                if (put_map.find(upd->elem1) == put_map.end()) {
                    get_set.insert(upd->elem1);
                }

                if (upd->type != transaction::NODE_DELETE_REQ) {
                    busy_single[0] = upd->elem1;
                } else {
                    del_set.emplace(upd->elem1);
                }
                break;

            case transaction::EDGE_DELETE_REQ:
            case transaction::EDGE_SET_PROPERTY:
                if (put_map.find(upd->elem2) == put_map.end()) {
                    get_set.insert(upd->elem2);
                }

                if (upd->type != transaction::EDGE_DELETE_REQ) {
                    busy_single[1] = upd->elem1;
                }
                busy_single[0] = upd->elem2;
                break;

            default:
                WDEBUG << "bad type" << std::endl;
        }

        for (int i = 0; i < 3; i++) {
            uint64_t &e = busy_single[i];
            if ((e < UINT64_MAX) && (deleted_elems.find(e) == deleted_elems.end())) {
                if (busy_elems.find(e) == busy_elems.end()) {
                    busy_elems[e] = 1;
                } else {
                    busy_elems[e]++;
                }
                tx->busy_elems.emplace_back(e);
                e = UINT64_MAX;
            } else if (e < UINT64_MAX) {
                success = false;
                break;
            }
        }

        if (!success) {
            // some elems deleted, unbusy all busy_elems and abort
            break;
        }
    }

    bool dist_lock = (tx->del_elems.size() > 0) && (NUM_VTS > 1);
    if (!success) {
        unbusy_elems(tx->busy_elems);
        if (dist_lock) {
            std::bitset<NUM_VTS> locks;
            locks.set();
            release_dist_del_locks(locks, tx->del_elems);
        }
        vts->busy_mtx.unlock();

#ifdef weaver_test_
        tx_count_mtx.lock();
        num_prep--;
        WDEBUG << "num prep " << num_prep << ", num outst " << num_comm << std::endl;
        tx_count_mtx.unlock();
#endif
        hstub->del_tx(tx->id);
        send_abort(tx->client_id);
        return;
    }

    vts->busy_mtx.unlock();

    // get mappings
    std::vector<std::pair<uint64_t, uint64_t>> get_map;
    if (!get_set.empty()) {
        get_map = nmstub->get_mappings(get_set);
        success = get_map.size() == get_set.size();
    }

    if (success) {
        // put and delete mappings
        assert(nmstub->put_mappings(put_map));
        assert(nmstub->del_mappings(del_set));
    }

    if (!success) {
        vts->busy_mtx.lock();
        unbusy_elems(tx->busy_elems);
        if (dist_lock) {
            std::bitset<NUM_VTS> locks;
            locks.set();
            release_dist_del_locks(locks, tx->del_elems);
        }
        vts->busy_mtx.unlock();

#ifdef weaver_test_
        tx_count_mtx.lock();
        num_prep--;
        WDEBUG << "num prep " << num_prep << ", num outst " << num_comm << std::endl;
        tx_count_mtx.unlock();
#endif
        hstub->del_tx(tx->id);
        send_abort(tx->client_id);
        return;
    }

    // all checks complete, tx has to succeed now

    std::unordered_map<uint64_t, uint64_t> all_map = std::move(put_map);
    for (auto &entry: get_map) {
        all_map.emplace(entry);
    }

    std::vector<transaction::pending_tx> tv(NUM_SHARDS, transaction::pending_tx());
    std::vector<bool> shard_write(NUM_SHARDS, false);
    int shard_count = 0;
    vts->clk_rw_mtx.wrlock();

    for (auto upd: tx->writes) {
        switch (upd->type) {

            case transaction::EDGE_CREATE_REQ:
                assert(all_map.find(upd->elem1) != all_map.end());
                assert(all_map.find(upd->elem2) != all_map.end());
                upd->loc1 = all_map[upd->elem1];
                upd->loc2 = all_map[upd->elem2];
                break;

            case transaction::NODE_DELETE_REQ:
            case transaction::NODE_SET_PROPERTY:
                assert(all_map.find(upd->elem1) != all_map.end());
                upd->loc1 = all_map[upd->elem1];
                break;

            case transaction::EDGE_DELETE_REQ:
            case transaction::EDGE_SET_PROPERTY:
                assert(all_map.find(upd->elem2) != all_map.end());
                upd->loc1 = all_map[upd->elem2];
                break;

            default:
                break;
        }

        uint64_t shard_idx = upd->loc1-SHARD_ID_INCR;
        vts->qts[shard_idx]++;
        upd->qts = vts->qts;
        tv[shard_idx].writes.emplace_back(upd);
        if (!shard_write[shard_idx]) {
            shard_count++;
            shard_write[shard_idx] = true;
        }
    }

    vts->vclk.increment_clock();
    tx->timestamp = vts->vclk;
    vts->clk_rw_mtx.unlock();

#ifdef weaver_test_
    tx_count_mtx.lock();
    num_prep--;
    num_comm++;
    WDEBUG << "num prep " << num_prep << ", num outst " << num_comm << std::endl;
    tx_count_mtx.unlock();
#endif

    // record txs as outstanding for reply bookkeeping and fault tolerance
    // XXX can leave elems locked if crash before this
    hstub->commit_tx(*tx);

    uint64_t tx_id = tx->id;
    vc::vclock tx_clk = tx->timestamp;
    vts->tx_prog_mutex.lock();
    vts->outstanding_tx.emplace(tx_id, current_tx(std::move(tx)));
    vts->outstanding_tx[tx_id].count = shard_count;
    vts->tx_prog_mutex.unlock();

    // send tx batches
    message::message msg;
    for (uint64_t i = 0; i < NUM_SHARDS; i++) {
        if (!tv[i].writes.empty()) {
            tv[i].timestamp = tx_clk;
            tv[i].id = tx_id;
            msg.prepare_message(message::TX_INIT, vt_id, tx_clk, tv[i].writes.at(0)->qts, tx_id, tv[i].writes);
            vts->comm.send(tv[i].writes.at(0)->loc1, msg.buf);
        }
    }
}

// if all replies have been received, ack to client
// also clean up tx state---local and distributed
void
end_transaction(uint64_t tx_id, coordinator::hyper_stub *hstub)
{
    vts->tx_prog_mutex.lock();
    if (--vts->outstanding_tx.at(tx_id).count == 0) {
        // done tx

#ifdef weaver_test_
        tx_count_mtx.lock();
        num_comm--;
        WDEBUG << "num prep " << num_prep << ", num outst " << num_comm << std::endl;
        tx_count_mtx.unlock();
#endif

        hstub->del_tx(tx_id);
        auto tx = std::move(vts->outstanding_tx[tx_id].tx);
        vts->outstanding_tx.erase(tx_id);
        vts->tx_prog_mutex.unlock();

        bool dist_lock = (tx->del_elems.size() > 0) && (NUM_VTS > 1);
        vts->busy_mtx.lock();
        unbusy_elems(tx->busy_elems);
        if (dist_lock) {
            unlock_del_elems(tx->del_elems);
        }
        vts->busy_mtx.unlock();

        // release distributed del locks
        message::message msg;
        if (dist_lock) {
            for (uint64_t i = 0; i < NUM_VTS; i++) {
                if (i == vt_id) {
                    continue;
                }
                msg.prepare_message(message::UNPREP_DEL_TX, tx->del_elems);
                vts->comm.send(i, msg.buf);
            }
        }

        // send response to client
        msg.prepare_message(message::CLIENT_TX_SUCCESS);
        vts->comm.send(tx->client_id, msg.buf);
    } else {
        vts->tx_prog_mutex.unlock();
    }
}

// single dedicated thread which wakes up after given timeout, sends updates, and sleeps
void
nop_function()
{
    timespec sleep_time;
    int sleep_ret;
    int sleep_flags = 0;
    vc::vclock vclk(vt_id, 0);
    vc::qtimestamp_t qts;
    uint64_t req_id, max_done_id;
    vc::vclock_t max_done_clk;
    uint64_t num_outstanding_progs;
    typedef std::vector<std::pair<uint64_t, node_prog::prog_type>> done_req_t;
    std::vector<done_req_t> done_reqs(NUM_SHARDS, done_req_t());
    std::vector<uint64_t> del_done_reqs;
    message::message msg;
    //bool nop_sent, clock_synced;

    sleep_time.tv_sec  = VT_TIMEOUT_NANO / NANO;
    sleep_time.tv_nsec = VT_TIMEOUT_NANO % NANO;

    while (true) {
        sleep_ret = clock_nanosleep(CLOCK_REALTIME, sleep_flags, &sleep_time, NULL);
        if (sleep_ret != 0 && sleep_ret != EINTR) {
            assert(false);
        }
        //nop_sent = false;
        //clock_synced = false;
        vts->periodic_update_mutex.lock();

        // send nops and state cleanup info to shards
        if (vts->to_nop.any()) {
            req_id = vts->generate_id();
            vts->clk_rw_mtx.wrlock();
            vts->vclk.increment_clock();
            vclk.clock = vts->vclk.clock;
            for (uint64_t shard_id = 0; shard_id < NUM_SHARDS; shard_id++) {
                if (vts->to_nop[shard_id]) {
                    vts->qts[shard_id]++;
                    done_reqs[shard_id].clear();
                }
            }
            qts = vts->qts;
            vts->clk_rw_mtx.unlock();

            del_done_reqs.clear();
            vts->tx_prog_mutex.lock();
            max_done_id = vts->max_done_id;
            max_done_clk = *vts->max_done_clk;
            num_outstanding_progs = vts->pend_prog_queue.size();
            for (auto &x: vts->done_reqs) {
                // x.first = node prog type
                // x.second = unordered_map <req_id -> bitset<NUM_SHARDS>>
                for (auto &reply: x.second) {
                    // reply.first = req_id
                    // reply.second = bitset<NUM_SHARDS>
                    for (uint64_t shard_id = 0; shard_id < NUM_SHARDS; shard_id++) {
                        if (vts->to_nop[shard_id] && !reply.second[shard_id]) {
                            reply.second.set(shard_id);
                            done_reqs[shard_id].emplace_back(std::make_pair(reply.first, x.first));
                        }
                    }
                    if (reply.second.all()) {
                        del_done_reqs.emplace_back(reply.first);
                    }
                }
                for (auto &del: del_done_reqs) {
                    x.second.erase(del);
                }
            }
            vts->tx_prog_mutex.unlock();

            for (uint64_t shard_id = 0; shard_id < NUM_SHARDS; shard_id++) {
                if (vts->to_nop[shard_id]) {
                    assert(vclk.clock.size() == NUM_VTS);
                    assert(max_done_clk.size() == NUM_VTS);
                    msg.prepare_message(message::VT_NOP, vt_id, vclk, qts, req_id,
                        done_reqs[shard_id], max_done_id, max_done_clk,
                        num_outstanding_progs, vts->shard_node_count);
                    vts->comm.send(shard_id + SHARD_ID_INCR, msg.buf);
                }
            }
            vts->to_nop.reset();
            //nop_sent = true;
        }

        // update vclock at other timestampers
        //if (vts->clock_update_acks == (NUM_VTS-1) && NUM_VTS > 1) {
        //clock_synced = true;
        //vts->clock_update_acks = 0;
        //if (!nop_sent) {
        //    vts->clk_mutex.lock();
        //    vclk.clock = vts->vclk.clock;
        //    vts->clk_mutex.unlock();
        //}
        //for (uint64_t i = 0; i < NUM_VTS; i++) {
        //    if (i == vt_id) {
        //        continue;
        //    }
        //    msg.prepare_message(message::VT_CLOCK_UPDATE, vt_id, vclk.clock[vt_id]);
        //    vts->comm.send(i, msg.buf);
        //}
        ////}

        //if (nop_sent && !clock_synced) {
        ////    WDEBUG << "nop yes, clock no" << std::endl;
        //} else if (!nop_sent && clock_synced) {
        ////    WDEBUG << "clock yes, nop no" << std::endl;
        //}

        vts->periodic_update_mutex.unlock();
    }
}

void
clk_update_function()
{
    timespec sleep_time;
    int sleep_ret;
    int sleep_flags = 0;
    message::message msg;
    vc::vclock vclk(vt_id, 0);

    sleep_time.tv_sec  = VT_CLK_TIMEOUT_NANO / NANO;
    sleep_time.tv_nsec = VT_CLK_TIMEOUT_NANO % NANO;

    while (true) {
        sleep_ret = clock_nanosleep(CLOCK_REALTIME, sleep_flags, &sleep_time, NULL);
        if (sleep_ret != 0 && sleep_ret != EINTR) {
            assert(false);
        }
        vts->periodic_update_mutex.lock();

        // update vclock at other timestampers
        vts->clk_rw_mtx.rdlock();
        vclk.clock = vts->vclk.clock;
        vts->clk_rw_mtx.unlock();
        for (uint64_t i = 0; i < NUM_VTS; i++) {
            if (i == vt_id) {
                continue;
            }
            msg.prepare_message(message::VT_CLOCK_UPDATE, vt_id, vclk.clock[vt_id]);
            vts->comm.send(i, msg.buf);
        }

        vts->periodic_update_mutex.unlock();
    }
}

// unpack client message for a node program, prepare shard msges, and send out
template <typename ParamsType, typename NodeStateType, typename CacheValueType>
void node_prog :: particular_node_program<ParamsType, NodeStateType, CacheValueType> :: 
    unpack_and_start_coord(std::unique_ptr<message::message> msg, uint64_t clientID, nmap::nmap_stub *nmap_cl)
{
    node_prog::prog_type pType;
    std::vector<std::pair<uint64_t, ParamsType>> initial_args;

    msg->unpack_message(message::CLIENT_NODE_PROG_REQ, pType, initial_args);
    
    // map from locations to a list of start_node_params to send to that shard
    std::unordered_map<uint64_t, std::deque<std::pair<uint64_t, ParamsType>>> initial_batches; 

    // lookup mappings
    std::unordered_map<uint64_t, uint64_t> request_element_mappings;
    std::unordered_set<uint64_t> get_set;
    for (auto &initial_arg : initial_args) {
        uint64_t c_id = initial_arg.first;
        get_set.insert(c_id);
    }
    if (!get_set.empty()) {
        auto results = nmap_cl->get_mappings(get_set);
        assert(results.size() == get_set.size());
        for (auto &toAdd : results) {
            request_element_mappings.emplace(toAdd);
        }
    }

    for (std::pair<uint64_t, ParamsType> &node_params_pair: initial_args) {
        uint64_t loc = request_element_mappings[node_params_pair.first];
        initial_batches[loc].emplace_back(std::make_pair(node_params_pair.first,
                    std::move(node_params_pair.second)));
    }
    
    vts->clk_rw_mtx.wrlock();
    vts->vclk.increment_clock();
    vc::vclock req_timestamp = vts->vclk;
    assert(req_timestamp.clock.size() == NUM_VTS);
    vts->clk_rw_mtx.unlock();

    vts->tx_prog_mutex.lock();
    uint64_t req_id = vts->generate_id();
    vts->outstanding_progs.emplace(req_id, current_prog(clientID, req_timestamp.clock));
    vts->pend_prog_queue.emplace(req_id);
    vts->tx_prog_mutex.unlock();

    message::message msg_to_send;
    for (auto &batch_pair : initial_batches) {
        msg_to_send.prepare_message(message::NODE_PROG, pType, vt_id, req_timestamp, req_id, batch_pair.second);
        vts->comm.send(batch_pair.first, msg_to_send.buf);
    }
}

template <typename ParamsType, typename NodeStateType, typename CacheValueType>
void node_prog :: particular_node_program<ParamsType, NodeStateType, CacheValueType> ::
    unpack_and_run_db(std::unique_ptr<message::message>)
{ }

template <typename ParamsType, typename NodeStateType, typename CacheValueType>
void node_prog :: particular_node_program<ParamsType, NodeStateType, CacheValueType> ::
    unpack_context_reply_db(std::unique_ptr<message::message>)
{ }

// remove a completed node program from outstanding requests data structure
// update 'max_done_id' and 'max_done_clk' accordingly
// caution: need to hold vts->tx_prog_mutex
void
mark_req_finished(uint64_t req_id)
{
    if (vts->pend_prog_queue.top() == req_id) {
        assert(vts->max_done_id < vts->pend_prog_queue.top());
        vts->max_done_id = req_id;
        auto outstanding_prog_iter = vts->outstanding_progs.find(vts->max_done_id);
        assert(outstanding_prog_iter != vts->outstanding_progs.end());
        vts->max_done_clk = std::move(outstanding_prog_iter->second.vclk);
        vts->pend_prog_queue.pop();
        vts->outstanding_progs.erase(vts->max_done_id);
        while (!vts->pend_prog_queue.empty() && !vts->done_prog_queue.empty()
            && vts->pend_prog_queue.top() == vts->done_prog_queue.top()) {
            assert(vts->max_done_id < vts->pend_prog_queue.top());
            vts->max_done_id = vts->pend_prog_queue.top();
            outstanding_prog_iter = vts->outstanding_progs.find(vts->max_done_id);
            assert(outstanding_prog_iter != vts->outstanding_progs.end());
            vts->max_done_clk = std::move(outstanding_prog_iter->second.vclk);
            vts->pend_prog_queue.pop();
            vts->done_prog_queue.pop();
            vts->outstanding_progs.erase(vts->max_done_id);
        }
    } else {
        vts->done_prog_queue.emplace(req_id);
    }
}

void
server_loop(int thread_id)
{
    busybee_returncode ret;
    enum message::msg_type mtype;
    std::unique_ptr<message::message> msg;
    uint64_t sender, tx_id;
    node_prog::prog_type pType;
    coordinator::hyper_stub *hstub = vts->hstub[thread_id];
    nmap::nmap_stub *nmstub = vts->nmap_client[thread_id];

    while (true) {
        vts->comm.quiesce_thread(thread_id);
        msg.reset(new message::message());
        ret = vts->comm.recv(&sender, &msg->buf);
        if (ret != BUSYBEE_SUCCESS && ret != BUSYBEE_TIMEOUT) {
            continue;
        } else {
            // good to go, unpack msg
            mtype = msg->unpack_message_type();
            //sender -= ID_INCR;

            switch (mtype) {
                // client messages
                case message::CLIENT_TX_INIT: {
                    std::unique_ptr<transaction::pending_tx> tx(new transaction::pending_tx());

                    if (!unpack_tx(*msg, sender, *tx)) {
                        // tx fail because multiple deletes for same node/edge
                        send_abort(sender);
                    }
                    prepare_tx_step1(std::move(tx), nmstub, hstub);

                    break;
                }

                case message::PREP_DEL_TX: {
                    std::unordered_set<uint64_t> del_elems;
                    uint64_t tx_id, tx_vt;
                    msg->unpack_message(message::PREP_DEL_TX, tx_vt, tx_id, del_elems);

                    vts->busy_mtx.lock();
                    bool success = lock_del_elems(tx_id, del_elems);
                    vts->busy_mtx.unlock();

                    if (success) {
                        msg->prepare_message(message::DONE_DEL_TX, tx_id, vt_id);
                        vts->comm.send(tx_vt, msg->buf);
                    } else {
                        msg->prepare_message(message::FAIL_DEL_TX, tx_id);
                        vts->comm.send(tx_vt, msg->buf);
                    }
                    break;
                }

                case message::UNPREP_DEL_TX: {
                    std::unordered_set<uint64_t> del_elems;
                    msg->unpack_message(message::UNPREP_DEL_TX, del_elems);

                    vts->busy_mtx.lock();
                    unlock_del_elems(del_elems);
                    vts->busy_mtx.unlock();

                    break;
                }

                case message::DONE_DEL_TX: {
                    uint64_t tx_id, from_vt;
                    msg->unpack_message(message::DONE_DEL_TX, tx_id, from_vt);

                    done_prepare_tx_step1(tx_id, from_vt, nmstub, hstub);
                    break;
                }

                case message::FAIL_DEL_TX: {
                    uint64_t tx_id;
                    msg->unpack_message(message::FAIL_DEL_TX, tx_id);

                    fail_prepare_tx_step1(tx_id, hstub);
                    break;
                }

                case message::VT_CLOCK_UPDATE: {
                    uint64_t rec_vtid, rec_clock;
                    msg->unpack_message(message::VT_CLOCK_UPDATE, rec_vtid, rec_clock);
                    vts->clk_rw_mtx.wrlock();
                    vts->clk_updates++;
                    vts->vclk.update_clock(rec_vtid, rec_clock);
                    vts->clk_rw_mtx.unlock();
                    //msg->prepare_message(message::VT_CLOCK_UPDATE_ACK);
                    //vts->comm.send(rec_vtid, msg->buf);
                    break;
                }

                //case message::VT_CLOCK_UPDATE_ACK:
                //    vts->periodic_update_mutex.lock();
                //    vts->clock_update_acks++;
                //    assert(vts->clock_update_acks < NUM_VTS);
                //    vts->periodic_update_mutex.unlock();
                //    break;

                case message::VT_NOP_ACK: {
                    uint64_t shard_node_count, nop_qts, sid;
                    msg->unpack_message(message::VT_NOP_ACK, sender, nop_qts, shard_node_count);
                    sid = sender - SHARD_ID_INCR;
                    vts->periodic_update_mutex.lock();
                    if (nop_qts > vts->nop_ack_qts[sid]) {
                        vts->shard_node_count[sid] = shard_node_count;
                        vts->to_nop.set(sid);
                        vts->nop_ack_qts[sid] = nop_qts;
                    }
                    vts->periodic_update_mutex.unlock();
                    break;
                }

                case message::CLIENT_MSG_COUNT: {
                    vts->msg_count_mutex.lock();
                    vts->msg_count = 0;
                    vts->msg_count_acks = 0;
                    vts->msg_count_mutex.unlock();
                    for (uint64_t i = SHARD_ID_INCR; i < (SHARD_ID_INCR + NUM_SHARDS); i++) {
                        msg->prepare_message(message::MSG_COUNT, vt_id);
                        vts->comm.send(i, msg->buf);
                    }
                    break;
                }

                case message::CLIENT_NODE_COUNT: {
                    vts->periodic_update_mutex.lock();
                    msg->prepare_message(message::NODE_COUNT_REPLY, vts->shard_node_count);
                    vts->periodic_update_mutex.unlock();
                    vts->comm.send(sender, msg->buf);
                    break;
                }

                // shard messages
                case message::LOADED_GRAPH: {
                    uint64_t load_time;
                    msg->unpack_message(message::LOADED_GRAPH, load_time);
                    vts->graph_load_mutex.lock();
                    if (load_time > vts->max_load_time) {
                        vts->max_load_time = load_time;
                    }
                    if (++vts->load_count == NUM_SHARDS) {
                        WDEBUG << "Graph loaded on all machines, time taken = " << vts->max_load_time << " nanosecs." << std::endl;
                    }
                    vts->graph_load_mutex.unlock();
                    break;
                }

                case message::TX_DONE:
                    msg->unpack_message(message::TX_DONE, tx_id);
                    end_transaction(tx_id, hstub);
                    break;

                case message::START_MIGR: {
                    uint64_t hops = UINT64_MAX;
                    msg->prepare_message(message::MIGRATION_TOKEN, hops, vt_id);
                    vts->comm.send(SHARD_ID_INCR, msg->buf); 
                    break;
                }

                case message::ONE_STREAM_MIGR: {
                    uint64_t hops = NUM_SHARDS;
                    vts->migr_mutex.lock();
                    vts->migr_client = sender;
                    vts->migr_mutex.unlock();
                    msg->prepare_message(message::MIGRATION_TOKEN, hops, vt_id);
                    vts->comm.send(SHARD_ID_INCR, msg->buf);
                    break;
                }

                case message::MIGRATION_TOKEN: {
                    vts->migr_mutex.lock();
                    uint64_t client = vts->migr_client;
                    vts->migr_mutex.unlock();
                    msg->prepare_message(message::DONE_MIGR);
                    vts->comm.send(client, msg->buf);
                    WDEBUG << "Shard node counts are:";
                    for (uint64_t &x: vts->shard_node_count) {
                        std::cerr << " " << x;
                    }
                    std::cerr << std::endl;
                    break;
                }

                case message::CLIENT_NODE_PROG_REQ:
                    msg->unpack_partial_message(message::CLIENT_NODE_PROG_REQ, pType);
                    node_prog::programs.at(pType)->unpack_and_start_coord(std::move(msg), sender, nmstub);
                    break;

                // node program response from a shard
                case message::NODE_PROG_RETURN: {
                    uint64_t req_id;
                    node_prog::prog_type type;
                    msg->unpack_partial_message(message::NODE_PROG_RETURN, type, req_id); // don't unpack rest
                    vts->tx_prog_mutex.lock();
                    auto outstanding_prog_iter = vts->outstanding_progs.find(req_id);
                    if (outstanding_prog_iter != vts->outstanding_progs.end()) { 
                        uint64_t client = outstanding_prog_iter->second.client;
                        vts->done_reqs[type].emplace(req_id, std::bitset<NUM_SHARDS>());
                        vts->comm.send(client, msg->buf);
                        mark_req_finished(req_id);
                    } else {
                        WDEBUG << "node prog return for already completed or never existed req id" << std::endl;
                    }
                    vts->tx_prog_mutex.unlock();
                    break;
                }

                case message::MSG_COUNT: {
                    uint64_t shard, msg_count;
                    msg->unpack_message(message::MSG_COUNT, shard, msg_count);
                    vts->msg_count_mutex.lock();
                    vts->msg_count += msg_count;
                    if (++vts->msg_count_acks == NUM_SHARDS) {
                        WDEBUG << "Msg count = " << vts->msg_count << std::endl;
                    }
                    vts->msg_count_mutex.unlock();
                    break;
                }

                default:
                    std::cerr << "unexpected msg type " << mtype << std::endl;
            }
        }
    }
}

void
server_manager_link_loop(po6::net::hostname sm_host)
{
    // Most of the following code has been 'borrowed' from
    // Robert Escriva's HyperDex.
    // see https://github.com/rescrv/HyperDex for the original code.

    vts->sm_stub.set_server_manager_address(sm_host.address.c_str(), sm_host.port);

    if (!vts->sm_stub.register_id(vts->server, *vts->comm.get_loc()))
    {
        return;
    }

    bool cluster_jump = false;

    while (!vts->sm_stub.should_exit())
    {
        vts->exit_mutex.lock();
        if (vts->to_exit) {
            vts->sm_stub.request_shutdown();
            vts->to_exit = false;
        }
        vts->exit_mutex.unlock();

        if (!vts->sm_stub.maintain_link())
        {
            continue;
        }
        const configuration& old_config(vts->config);
        const configuration& new_config(vts->sm_stub.config());

        if (old_config.cluster() != 0 &&
            old_config.cluster() != new_config.cluster())
        {
            cluster_jump = true;
            break;
        }

        if (old_config.version() > new_config.version())
        {
            WDEBUG << "received new configuration version=" << new_config.version()
                   << " that's older than our current configuration version="
                   << old_config.version();
            continue;
        }
        // if old_config.version == new_config.version, still fetch

        vts->config_mutex.lock();
        vts->config = new_config;
        if (!vts->first_config) {
            vts->first_config = true;
            vts->first_config_cond.signal();
        } else {
            while (!vts->vts_init) {
                vts->vts_init_cond.wait();
            }
            vts->reconfigure();
        }
        vts->config_mutex.unlock();

        // let the coordinator know we've moved to this config
        vts->sm_stub.config_ack(new_config.version());
    }

    if (cluster_jump)
    {
        WDEBUG << "\n================================================================================\n"
               << "Exiting because the server manager changed on us.\n"
               << "This is most likely an operations error."
               << "================================================================================";
    }
    else if (vts->sm_stub.should_exit() && !vts->sm_stub.config().exists(vts->server))
    {
        WDEBUG << "\n================================================================================\n"
               << "Exiting because the server manager says it doesn't know about this node.\n"
               << "================================================================================";
    }
    else if (vts->sm_stub.should_exit())
    {
        WDEBUG << "\n================================================================================\n"
               << "Exiting because server manager stub says we should exit.\n"
               << "Most likely because we requested shutdown due to program interrupt.\n"
               << "================================================================================\n";
    }
    exit(0);
}

void
install_signal_handler(int signum, void (*handler)(int))
{
    struct sigaction sa;
    sa.sa_handler = handler;
    sigfillset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    int ret = sigaction(signum, &sa, NULL);
    assert(ret == 0);
}

int
main(int argc, char *argv[])
{
    if (argc < 2 || argc > 3) {
        WDEBUG << "Usage,   primary vt:" << argv[0] << " <vector_timestamper_id>" << std::endl
               << "          backup vt:" << argv[0] << " <vector_timestamper_id> <backup_number>" << std::endl; 
        return -1;
    }

#ifdef weaver_test_
    num_prep = 0;
    num_comm = 0;
#endif

    install_signal_handler(SIGINT, end_program);
    install_signal_handler(SIGHUP, end_program);
    install_signal_handler(SIGTERM, end_program);
    install_signal_handler(SIGTSTP, end_program);

    sigset_t ss;
    if (sigfillset(&ss) < 0) {
        WDEBUG << "sigfillset failed" << std::endl;
        return -1;
    }
    sigdelset(&ss, SIGPROF);
    if (pthread_sigmask(SIG_SETMASK, &ss, NULL) < 0) {
        WDEBUG << "pthread sigmask failed" << std::endl;
        return -1;
    }

    // vt setup
    vt_id = atoi(argv[1]);
    if (argc == 3) {
        vts = new coordinator::timestamper(vt_id, atoi(argv[2]));
        assert((atoi(argv[2]) - vt_id) % (NUM_EFFECTIVE_SERVERS) == 0);
    } else {
        vts = new coordinator::timestamper(vt_id, vt_id);
    }

    // server manager link
    std::thread sm_thr(server_manager_link_loop,
        po6::net::hostname(SERVER_MANAGER_IPADDR, SERVER_MANAGER_PORT));
    sm_thr.detach();

    vts->config_mutex.lock();

    // wait for first config to arrive from server manager
    while (!vts->first_config) {
        vts->first_config_cond.wait();
    }

    // registered this server with server_manager, config has fairly recent value
    vts->init();
    vts->vts_init = true;
    vts->vts_init_cond.signal();

    vts->config_mutex.unlock();

    // start all threads
    std::thread *thr;
    for (int i = 0; i < NUM_THREADS; i++) {
        thr = new std::thread(server_loop, i);
        thr->detach();
    }

    if (argc == 3) {
        // wait till this server becomes primary vt
        vts->config_mutex.lock();
        while (!vts->active_backup) {
            vts->backup_cond.wait();
        }
        vts->config_mutex.unlock();
        WDEBUG << "backup " << atoi(argv[2]) << " now primary for vt " << vt_id << std::endl;
        vts->restore_backup();
    } else {
        // this server is primary vt, start now
        std::cout << "Vector timestamper " << vt_id << std::endl;
    }

    // initial wait for all vector timestampers to start
    // TODO change this to use config pushed by server manager
    timespec sleep_time;
    sleep_time.tv_sec =  INITIAL_TIMEOUT_NANO / NANO;
    sleep_time.tv_nsec = INITIAL_TIMEOUT_NANO % NANO;
    int ret = clock_nanosleep(CLOCK_REALTIME, 0, &sleep_time, NULL);
    assert(ret == 0);
    WDEBUG << "Initial setup delay complete" << std::endl;

    UNUSED(ret);

    // periodic vector clock update to other timestampers
    std::thread clk_update_thr(clk_update_function);
    clk_update_thr.detach();

    // periodic nops to shard
    nop_function();
}

#undef weaver_test_
