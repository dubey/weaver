/*
 * ===============================================================
 *    Description:  Wrapper around Busybee object.
 *
 *        Created:  05/22/2013 04:23:55 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_common_comm_wrapper_h_
#define weaver_common_comm_wrapper_h_

#include <fstream>
#include <algorithm>
#include <unordered_map>
#include <busybee_constants.h>
#include <busybee_mapper.h>
#include <busybee_mta.h>

#include "weaver_constants.h"
#include "configuration.h"

namespace common
{

class comm_wrapper
{
    public:
        // map from server ids -> po6 locs
        class weaver_mapper : public busybee_mapper
        {
            private:
                std::unordered_map<uint64_t, po6::net::location> mlist; // active servers only
                uint64_t active_server_idx[NUM_VTS+NUM_SHARDS]; // for each vt/shard, index of the active server
                configuration config;

            public:
                weaver_mapper();
                virtual ~weaver_mapper() throw () {}
                virtual bool lookup(uint64_t server_id, po6::net::location *loc);
                void reconfigure(configuration &new_config, uint64_t &now_primary);
                void client_configure(std::unordered_map<uint64_t, po6::net::location> cluster) { mlist = cluster; }

            private:
                weaver_mapper(const weaver_mapper&);
                weaver_mapper& operator=(const weaver_mapper&);
        };

    private:
        std::unique_ptr<busybee_mta> bb;
        std::unique_ptr<weaver_mapper> wmap;
        std::shared_ptr<po6::net::location> loc;
        std::unordered_map<uint64_t, po6::net::location> cluster;
        uint64_t bb_id;
        int num_threads;
        int timeout;

    public:
        comm_wrapper(uint64_t bbid, int nthr, int timeout);
        void init(configuration &config);
        void client_init();
        uint64_t reconfigure(configuration &config);
        std::shared_ptr<po6::net::location> get_loc() { return loc; }
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
        busybee_returncode send(uint64_t send_to, std::auto_ptr<e::buffer> msg);
        busybee_returncode recv(uint64_t *recv_from, std::auto_ptr<e::buffer> *msg);
#pragma GCC diagnostic pop
};

}
#endif
