/*
 * ===============================================================
 *    Description:  Coordinator link wrapper for shards.
 *
 *        Created:  2014-02-10 14:52:59
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

#ifndef weaver_shard_server_manager_link_wrapper_h_
#define weaver_shard_server_manager_link_wrapper_h_

// C
#include <stdint.h>
#include <map>
#include <queue>

// po6
#include <po6/net/location.h>
#include <po6/threads/thread.h>
#include <po6/threads/cond.h>
#include <po6/threads/mutex.h>

// e
#include <e/intrusive_ptr.h>

// Weaver
#include "common/configuration.h"
#include "common/server_manager_link.h"
#include "common/ids.h"

// The thread whose calls the constructor can call everything.  All other
// threads are left with the threadsafe block below.

class server_manager_link_wrapper
{
    public:
        server_manager_link_wrapper(server_id us, std::shared_ptr<po6::net::location> loc);
        ~server_manager_link_wrapper() throw ();

    public:
        void set_server_manager_address(const char* host, uint16_t port);
        bool get_unique_number(uint64_t &id);
        bool register_id(server_id us, const po6::net::location& bind_to, server::type_t type);
        bool should_exit();
        bool maintain_link();
        const configuration& config();
        void request_shutdown();

    // threadsafe
    public:
        void config_ack(uint64_t version);
        void config_stable(uint64_t version);
        void report_tcp_disconnect(uint64_t id);

    private:
        class sm_rpc;
        class sm_rpc_available;
        class sm_rpc_config_ack;
        class sm_rpc_config_stable;
        typedef std::map<int64_t, e::intrusive_ptr<sm_rpc> > rpc_map_t;

    private:
        void background_maintenance();
        void do_sleep();
        void reset_sleep();
        void enter_critical_section();
        void exit_critical_section();
        void enter_critical_section_killable();
        void enter_critical_section_background();
        void exit_critical_section_killable();
        void ensure_available();
        void ensure_config_ack();
        void ensure_config_stable();
        void make_rpc(const char* func,
                      const char* data, size_t data_sz,
                      e::intrusive_ptr<sm_rpc> rpc);
        int64_t make_rpc_nosync(const char* func,
                                const char* data, size_t data_sz,
                                e::intrusive_ptr<sm_rpc> rpc);
        int64_t make_rpc_defended(const char* enter_func,
                                  const char* enter_data, size_t enter_data_sz,
                                  const char* exit_func,
                                  const char* exit_data, size_t exit_data_sz,
                                  e::intrusive_ptr<sm_rpc> rpc);
        int64_t wait_nosync(const char* cond, uint64_t state,
                            e::intrusive_ptr<sm_rpc> rpc);

    private:
        server_id m_us;
        std::shared_ptr<po6::net::location> m_loc;
        po6::threads::thread m_poller;
        std::queue<std::pair<int64_t, replicant_returncode>> m_deferred;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
        std::auto_ptr<server_manager_link> m_sm;
#pragma GCC diagnostic pop
        rpc_map_t m_rpcs;
        po6::threads::mutex m_mtx;
        po6::threads::cond m_cond;
        bool m_poller_started;
        bool m_locked;
        bool m_kill;
        pthread_t m_to_kill;
        uint64_t m_waiting;
        uint64_t m_sleep;
        int64_t m_online_id;
        bool m_shutdown_requested;
        // make sure we reliably ack
        bool m_need_config_ack;
        uint64_t m_config_ack;
        int64_t m_config_ack_id;
        // make sure we reliably stabilize
        bool m_need_config_stable;
        uint64_t m_config_stable;
        int64_t m_config_stable_id;

    private:
        server_manager_link_wrapper(const server_manager_link_wrapper&);
        server_manager_link_wrapper& operator = (const server_manager_link_wrapper&);
};

#endif // weaver_shard_server_manager_link_wrapper_h_
