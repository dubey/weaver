/*
 * ===============================================================
 *    Description:  Replicated server manager which monitors all
 *                  servers for liveness and broadcasts new
 *                  configuration on failure.
 *
 *        Created:  2014-02-08 13:46:07
 *
 *         Author:  Robert Escriva, escriva@cs.cornell.edu
 *                  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

// Most of the following code has been 'borrowed' from
// Robert Escriva's HyperDex coordinator.
// see https://github.com/rescrv/HyperDex for the original code.

#ifndef weaver_server_manager_server_manager_h_
#define weaver_server_manager_server_manager_h_

// c++
#include <memory>

// po6
#include <po6/net/location.h>

// Replicant
#include <rsm.h>

// Weaver
#include "common/ids.h"
#include "common/server.h"
#include "coordinator/server_barrier.h"

namespace coordinator
{

class server_manager
{
    public:
        server_manager();
        ~server_manager() throw ();

    // identity
    public:
        void init(rsm_context* ctx, uint64_t token);
        uint64_t cluster() const { return m_cluster; }

    // server management
    public:
        void server_register(rsm_context* ctx,
                             const server_id& sid,
                             const po6::net::location& bind_to,
                             server::type_t type);
        void server_online(rsm_context* ctx,
                           const server_id& sid,
                           const po6::net::location* bind_to);
        void server_offline(rsm_context* ctx,
                            const server_id& sid);
        void server_shutdown(rsm_context* ctx,
                             const server_id& sid);
        void server_kill(rsm_context* ctx,
                         const server_id& sid);
        void server_forget(rsm_context* ctx,
                           const server_id& sid);
        void server_suspect(rsm_context* ctx,
                            const server_id& sid);
        void report_disconnect(rsm_context* ctx,
                               const server_id& sid, uint64_t version);

    private:
        void check_backup(rsm_context *ctx, server *new_srv);
        void find_backup(rsm_context *ctx, server::type_t type, uint64_t vid);
        void activate_backup(server *backup_srv, server::type_t type, uint64_t vid);

    // config management
    public:
        void config_get(rsm_context* ctx);
        void config_ack(rsm_context* ctx,
                        const server_id& sid, uint64_t version);
        void config_stable(rsm_context* ctx,
                           const server_id& sid, uint64_t version);

    // debug
    public:
        void debug_dump(rsm_context* ctx);

    // backup/restore
    public:
        static server_manager* recreate(rsm_context* ctx,
                                        const char* data, size_t data_sz);
        int snapshot(rsm_context* ctx,
                     char** data, size_t* data_sz);

    // utilities
    private:
        // servers
        server* new_server(const server_id& sid);
        server* get_server(const server_id& sid);
        // configuration
        void check_ack_condition(rsm_context* ctx);
        void check_stable_condition(rsm_context* ctx);
        void generate_next_configuration(rsm_context* ctx);
        void generate_cached_configuration(rsm_context* ctx);
        void servers_in_configuration(std::vector<server_id>* sids);

    private:
        // meta state
        uint64_t m_cluster;
        uint64_t m_counter;
        uint64_t m_version;
        uint64_t m_flags;
        uint64_t m_num_shards;
        uint64_t m_num_vts;
        uint64_t m_num_weaver;
        // servers
        std::vector<server> m_servers;
        // barriers
        uint64_t m_config_ack_through;
        server_barrier m_config_ack_barrier;
        uint64_t m_config_stable_through;
        server_barrier m_config_stable_barrier;
        // cached config
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
        std::auto_ptr<e::buffer> m_latest_config;
#pragma GCC diagnostic pop
        // for returning client id
        std::unique_ptr<e::buffer> m_response;

    private:
        server_manager(const server_manager&);
        server_manager& operator = (const server_manager&);
};

}

#endif
