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
#include <signal.h>

#include "common/vclock.h"
#include "timestamper.h"
#include "transaction.h"

static coordinator::timestamper *vts;
static uint64_t vt_id;

// SIGINT handler
void
end_program(int param)
{
    std::cerr << "Ending program, param = " << param << std::endl;
    exit(0);
}

// expects an input of list of writes that are part of this transaction
// for all writes, node mapper lookups should have already been performed
// for create requests, instead of lookup an entry for new handle should have been inserted
inline void
begin_transaction(coordinator::pending_tx &tx)
{
    message::message msg;
    vc::vclock_t clock;
    vts->mutex.lock();
    for (std::shared_ptr<coordinator::pending_update> upd: tx.writes) {
        switch (upd->type) {
            case message::NODE_CREATE_REQ:
            case message::NODE_DELETE_REQ:
                vts->qts.at(upd->loc1-1)++;
                break;

            case message::EDGE_CREATE_REQ:
            case message::EDGE_DELETE_REQ:
                vts->qts.at(upd->loc1-1)++;
                break;

            default:
                DEBUG << "bad update type";
        }
        upd->qts = vts->qts;
    }
    vts->vclk.increment_clock();
    tx.timestamp = vts->vclk.get_clock();
    vts->mutex.unlock();
    message::prepare_tx_message(msg, vt_id, tx);
    vts->send(tx.writes.at(0).loc1, msg);
}

void
server_loop()
{
    busybee_returncode ret;
    uint32_t code;
    enum message::msg_type mtype;
    std::unique_ptr<message::message> msg;
    uint64_t sender;

    while (true)
    {
        msg.reset(new message::message());
        if ((ret = vts->bb->recv(&sender, &msg->buf)) != BUSYBEE_SUCCESS) {
            std::cerr << "msg recv error: " << ret << std::endl;
            continue;
        }
        msg->buf->unpack_from(BUSYBEE_HEADER_SIZE) >> code;
        mtype = (enum message::msg_type)code;
        sender -= ID_INCR;

        switch (mtype) {
       
        // client messages
        case message::CLIENT_TX_INIT:
            coordinator::pending_tx tx;
            vts->unpack_tx(*msg, tx);
            begin_tx(tx);
            break;

        case message::VT_CLOCK_UPDATE: {
            uint64_t rec_vtid;
            std::vector<uint64_t> rec_clock;
            message::unpack_message(*msg, message::VT_CLOCK_UPDATE,
                rec_vtid, rec_clock);
            vts->mutex.lock();
            vts->vc.update_clock(rec_vtid, rec_clock);
            vts->mutex.unlock();
            break;
        }

        default:
            std::cerr << "unexpected msg type " << mtype << std::endl;
        }
    }
}

// periodically share local vector clock with other timestampers
// TODO permanent deletion of deleted and migrated nodes and state for completed node progs
inline void
coord_daemon_initiate()
{
    message::message msg;
    vclock::nvc clock;
    vts->mutex.lock();
    clock = vtc->vc.get_clock();
    vts->mutex.unlock();
    message::prepare_message(msg, message::VT_CLOCK_UPDATE, vt_id, clock);
    // broadcast updated vector clock
    for (uint64_t i = 0; i < NUM_VTS; i++) {
        if (i == vt_id) {
            continue;
        }
        vts->send(i, msg.buf);
    }
}

int
main(int argc, char *argv[])
{
    std::thread *thr;
    signal(SIGINT, end_program);
    if (argc != 2) {
        DEBUG << "Usage: " << argv[0] << " <vector_timestamper_id>" << std::endl;
        return -1;
    }
    vt_id = atoi(argv[1]);
    vts = new coordinator::timestamper(id);
    for (int i = 0; i < NUM_THREADS-1; i++) {
        thr = new std::thread(server_loop);
        thr->detach();
    }
    server_loop();
}
