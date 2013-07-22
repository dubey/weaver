/*
 * ===============================================================
 *    Description:  Server loop for state manager
 *
 *        Created:  07/12/2013 12:03:36 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <signal.h>
#include "state_manager.h"

static coordinator::state_manager state;

void
exit_weaver()
{
    exit(0);
}

// serve queued messages from request handlers
void
handle_msg(std::unique_ptr<message::message> msg, enum message::msg_type m_type, uint64_t sender)
{
    uint64_t req_id;

    switch(m_type) {
        case message::COORD_LOC_REQ: {
            uint64_t elem;
            message::unpack_message(*msg, m_type, req_id, elem);
            common::meta_element ret = state.check_and_get_loc(elem);
            message::prepare_message(*msg, message::COORD_LOC_REPLY, req_id, ret);
            state.send(sender, msg->buf);
            break;
        }

        case message::EXIT_WEAVER:
            exit_weaver();

        default:
            DEBUG << "unexpected msg type " << m_type << std::endl;
    }
}

// handle incoming messages
void
msg_handler()
{
    busybee_returncode ret;
    uint32_t code;
    enum message::msg_type mtype;
    std::unique_ptr<message::message> rec_msg;
    uint64_t sender;
    std::unique_ptr<coordinator::thread::unstarted_thread> thr;

    while (1)
    {
        rec_msg.reset(new message::message());
        if ((ret = state.bb->recv(&sender, &rec_msg->buf)) != BUSYBEE_SUCCESS) {
            std::cerr << "msg recv error: " << ret << std::endl;
            continue;
        }
        rec_msg->buf->unpack_from(BUSYBEE_HEADER_SIZE) >> code;
        mtype = (enum message::msg_type)code;
        sender -= ID_INCR;
        thr.reset(new coordinator::thread::unstarted_thread(handle_msg, std::move(rec_msg), mtype, sender));
        state.thread_pool.add_request(std::move(thr));
    }
}

void
end_program(int param)
{
    DEBUG << "Ending program, param = " << param << std::endl;
    exit_weaver();
}

int
main()
{
    signal(SIGINT, end_program);

    std::cout << "Weaver: coordinator state" << std::endl;

    msg_handler();
}
