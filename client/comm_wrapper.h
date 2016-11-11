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
#include <busybee_constants.h>
#include <busybee_mapper.h>
#include <busybee_st.h>
//#include <busybee_single.h>

#include "common/configuration.h"

namespace cl
{

class comm_wrapper
{
    public:
        class const_mapper : public busybee_mapper
        {
            private:
                po6::net::location m_loc;

            public:
                const_mapper(const char* ip_addr);
                virtual ~const_mapper() throw () { }
                virtual bool lookup(uint64_t server_id, po6::net::location *loc);

            private:
                const_mapper(const const_mapper&);
                const_mapper& operator=(const const_mapper&);
        };

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
        std::unique_ptr<weaver_mapper> wmap;
        std::unique_ptr<const_mapper> cmap;
        std::unique_ptr<busybee_st> m_bb;
        //std::unique_ptr<busybee_single> m_bb;
        uint64_t m_recv_from;

    public:
        comm_wrapper(const configuration &config);
        comm_wrapper(const char *ipaddr);
        ~comm_wrapper();
        void reconfigure(const configuration &new_config);
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
        busybee_returncode send(uint64_t send_to, std::auto_ptr<e::buffer> msg);
        busybee_returncode recv(std::auto_ptr<e::buffer> *msg);
#pragma GCC diagnostic pop
};

}

#endif
