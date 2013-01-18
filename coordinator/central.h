/*
 * ===============================================================
 *
 *    Description:  Coordinator class
 *
 *        Created:  10/27/2012 05:20:01 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __CENTRAL__
#define __CENTRAL__

#include <vector>
#include <busybee_sta.h>
#include <po6/net/location.h>

#include "common/meta_element.h"
#include "common/weaver_constants.h"
#include "common/vclock/vclock.h"

namespace coordinator
{
    class central
    {
        public:
            central();
        private:
            po6::net::location myloc;

        public:
            busybee_sta bb;
            busybee_sta rec_bb;
            uint32_t time;
            int port_ctr;
            std::vector<common::meta_element *> nodes;
            std::vector<common::meta_element *> edges;
            vclock::vector vc;
            po6::threads::mutex update_mutex;
            void add_node(common::meta_element *n);
            void add_edge(common::meta_element *e);
            busybee_returncode send(po6::net::location loc, std::auto_ptr<e::buffer> buf);
    };

    inline
    central :: central()
        : myloc(COORD_IPADDR, COORD_PORT)
        , bb(myloc.address, myloc.port, 0)
        , rec_bb(myloc.address, COORD_REC_PORT, 0)
    {
        time = 0;
        port_ctr = COORD_PORT;
    }

    inline void
    central :: add_node(common::meta_element *n)
    {
        update_mutex.lock();
        nodes.push_back(n);
        update_mutex.unlock();
    }

    inline void
    central :: add_edge(common::meta_element *e)
    {
        update_mutex.lock();
        edges.push_back(e);
        update_mutex.unlock();
    }

    inline busybee_returncode
    central :: send(po6::net::location loc, std::auto_ptr<e::buffer> buf)
    {
        busybee_returncode ret;
        if ((ret = bb.send(loc, buf)) != BUSYBEE_SUCCESS)
        {
            std::cerr << "message sending error " << ret << std::endl;
        }
        return ret;
    }
}

#endif // __CENTRAL__
