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

#include <unordered_map>
#include <e/garbage_collector.h>
#include <busybee_constants.h>
#include <busybee_mapper.h>
#include <busybee_mta.h>

#include "common/configuration.h"
#include "common/server_manager_link_wrapper.h"

namespace common
{

class comm_wrapper
{
    public:
        // map from server ids -> po6 locs
        class weaver_mapper : public busybee_mapper
        {
            private:
                std::unordered_map<uint64_t, po6::net::location> mlist;

            public:
                weaver_mapper() : mlist() { }
                virtual ~weaver_mapper() throw () { }
                virtual bool lookup(uint64_t server_id, po6::net::location *loc);
                void add_mapping(uint64_t server_id, const po6::net::location &loc);

            private:
                weaver_mapper(const weaver_mapper&);
                weaver_mapper& operator=(const weaver_mapper&);
        };

    private:
        std::vector<uint64_t> active_server_idx; // for each vt/shard, index of the active server
        configuration config;
        e::garbage_collector bb_gc;
        std::vector<std::unique_ptr<e::garbage_collector::thread_state>> bb_gc_ts;
        std::unique_ptr<busybee_mta> bb;
        std::unique_ptr<weaver_mapper> wmap;
        std::shared_ptr<po6::net::location> loc;
        uint64_t bb_id;
        int num_threads;
        int timeout;
        server_manager_link_wrapper *sm_stub;
        void reconfigure_internal(configuration&);
        void handle_disruption(uint64_t id);

    public:
        comm_wrapper(std::shared_ptr<po6::net::location> loc, int nthr, int timeout, server_manager_link_wrapper*);
        ~comm_wrapper();
        void reconfigure(configuration &config, bool to_pause=true, uint64_t *num_active_vts=nullptr);
        std::shared_ptr<po6::net::location> get_loc() { return loc; }
#ifdef weaver_async_node_recovery_
        int fd() { return bb->poll_fd(); }
#endif
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
        busybee_returncode send(uint64_t send_to, std::auto_ptr<e::buffer> msg);
        busybee_returncode send_to_client(uint64_t send_to, std::auto_ptr<e::buffer> msg);
        busybee_returncode recv(int tid, std::auto_ptr<e::buffer> *msg);
        busybee_returncode recv(int tid, uint64_t *recv_from, std::auto_ptr<e::buffer> *msg);
#pragma GCC diagnostic pop
        void quiesce_thread(int tid);
};

}
#endif
