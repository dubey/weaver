/*
 * ===============================================================
 *    Description:  Client busybee wrapper.
 *
 *        Created:  2014-07-02 15:58:10
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_client_comm_wrapper_h_
#define weaver_client_comm_wrapper_h_

#include <unordered_map>
#include <e/garbage_collector.h>
#include <busybee_constants.h>
#include <busybee_mapper.h>
#include <busybee_st.h>

#include "common/configuration.h"

namespace cl
{

class comm_wrapper
{
    public:
        class weaver_mapper : public busybee_mapper
        {
            private:
                std::unordered_map<uint64_t, po6::net::location> mlist;

            public:
                weaver_mapper(const configuration &config);
                virtual ~weaver_mapper() throw () { }
                virtual bool lookup(uint64_t server_id, po6::net::location *loc);

            private:
                weaver_mapper(const weaver_mapper&);
                weaver_mapper& operator=(const weaver_mapper&);
        };

    private:
        configuration config;
        e::garbage_collector bb_gc;
        e::garbage_collector::thread_state bb_gc_ts;
        std::unique_ptr<weaver_mapper> wmap;
        std::unique_ptr<busybee_st> bb;
        uint64_t bb_id;
        uint64_t recv_from;

    public:
        comm_wrapper(uint64_t bb_id, const configuration &config);
        ~comm_wrapper();
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
        busybee_returncode send(uint64_t send_to, std::auto_ptr<e::buffer> msg);
        busybee_returncode recv(std::auto_ptr<e::buffer> *msg);
#pragma GCC diagnostic pop
        void quiesce_thread();
        void drop();
};

}

#endif
