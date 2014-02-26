/*
 * ===============================================================
 *    Description:  Wrapper around common::message, for a queued
 *                  request.
 *
 *        Created:  2014-02-20 18:56:42
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013-2014, Cornell University, see the LICENSE
 *                     file for licensing agreement
 * ===============================================================
 */

#ifndef weaver_db_message_wrapper_h_
#define weaver_db_message_wrapper_h_

#include "common/message.h"

namespace db
{
    class message_wrapper
    {
        public:
            message_wrapper(enum message::msg_type mt, std::unique_ptr<message::message> m)
                : type(mt)
                , msg(std::move(m))
            { }

        public:
            enum message::msg_type type;
            std::unique_ptr<message::message> msg;
    };
}

#endif
