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
#include <random> // XXX for testing
#include <chrono>
#include <thread>
#include <busybee_sta.h>
#include <po6/net/location.h>

#include "common/meta_element.h"
#include "common/weaver_constants.h"
#include "common/vclock.h"

namespace coordinator
{
    class central
    {
        public:
            central();

        public:
            std::shared_ptr<po6::net::location> myloc;
            std::shared_ptr<po6::net::location> myrecloc;
            std::shared_ptr<po6::net::location> clientloc;
            busybee_sta bb;
            busybee_sta rec_bb;
            busybee_sta client_send_bb;
            busybee_sta client_rec_bb;
            po6::threads::mutex bb_mutex;
            po6::threads::mutex client_bb_mutex;
            int port_ctr;
            std::vector<std::shared_ptr<po6::net::location>> shards;
            std::vector<common::meta_element *> nodes;
            std::vector<common::meta_element *> edges;
            vclock::vector vc;
            po6::threads::mutex update_mutex;
            std::default_random_engine generator;
            std::uniform_real_distribution<double> dist;

        public:
            void add_node(common::meta_element *n);
            void add_edge(common::meta_element *e);
            busybee_returncode send(po6::net::location loc, std::auto_ptr<e::buffer> buf);
            busybee_returncode send(std::shared_ptr<po6::net::location> loc,
                std::auto_ptr<e::buffer> buf);
            busybee_returncode client_send(std::shared_ptr<po6::net::location> loc,
                std::auto_ptr<e::buffer> buf);
            busybee_returncode flaky_send(std::shared_ptr<po6::net::location> loc,
                std::auto_ptr<e::buffer> buf, bool delay);
    };

    inline
    central :: central()
        : myloc(new po6::net::location(COORD_IPADDR, COORD_PORT))
        , myrecloc(new po6::net::location(COORD_IPADDR, COORD_REC_PORT))
        , client_send_loc(new po6::net::location(COORD_IPADDR, COORD_CLIENT_SEND_PORT))
        , client_rec_loc(new po6::net::location(COORD_IPADDR, COORD_CLIENT_REC_PORT))
        , bb(myloc->address, myloc->port, 0)
        , rec_bb(myrecloc->address, myrecloc->port, 0)
        , client_send_bb(client_send_loc->address, client_send_loc->port, 0)
        , client_rec_bb(client_rec_loc->address, client_rec_loc->port, 0)
        , port_ctr(0)
        , generator((unsigned)42) // fixed seed for deterministic random numbers
        , dist(0.0, 1.0)
    {
        int i;
        // initialize array of shard server locations
        for (i = 1; i <= NUM_SHARDS; i++)
        {
            auto new_shard = std::make_shared<po6::net::location>(SHARD_IPADDR, COORD_PORT + i);
            shards.push_back(new_shard);
        }
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
        bb_mutex.lock();
        if ((ret = bb.send(loc, buf)) != BUSYBEE_SUCCESS)
        {
            std::cerr << "message sending error " << ret << std::endl;
        }
        bb_mutex.unlock();
        return ret;
    }

    inline busybee_returncode
    central :: send(std::shared_ptr<po6::net::location> loc, std::auto_ptr<e::buffer> buf)
    {
        busybee_returncode ret;
        bb_mutex.lock();
        if ((ret = bb.send(*loc, buf)) != BUSYBEE_SUCCESS)
        {
            std::cerr << "message sending error " << ret << std::endl;
        }
        bb_mutex.unlock();
        return ret;
    }
    
    inline busybee_returncode
    central :: client_send(std::shared_ptr<po6::net::location> loc, std::auto_ptr<e::buffer> buf)
    {
        busybee_returncode ret;
        client_bb_mutex.lock();
        if ((ret = bb.send(*loc, buf)) != BUSYBEE_SUCCESS)
        {
            std::cerr << "message sending error " << ret << std::endl;
        }
        client_bb_mutex.unlock();
        return ret;
    }
    
    inline busybee_returncode
    central :: flaky_send(std::shared_ptr<po6::net::location> loc,
        std::auto_ptr<e::buffer> buf, bool delay)
    {
        busybee_returncode ret;
        if (dist(generator) <= 1.0 && delay) // 100% messages delayed
        {
            std::chrono::seconds duration(10);
            std::this_thread::sleep_for(duration);
        }
        bb_mutex.lock();
        if ((ret = bb.send(*loc, buf)) != BUSYBEE_SUCCESS)
        {
            std::cerr << "message sending error " << ret << std::endl;
        }
        bb_mutex.unlock();
        return ret;
    }
}

#endif // __CENTRAL__
