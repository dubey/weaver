/*
 * ===============================================================
 *    Description:  Class definition for wrapper around replicant
 *                  client of server manager.
 *
 *        Created:  2014-02-10 12:59:53
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

#ifndef weaver_common_server_manager_link_h_
#define weaver_common_server_manager_link_h_

#define __STDC_LIMIT_MACROS

// C
#include <stdint.h>

// STL
#include <list>

// e
#include <e/error.h>

// Replicant
#include <replicant.h>

// Weaver
#include "common/configuration.h"

class server_manager_link
{
    public:
#ifdef _MSC_VER
        typedef fd_set* poll_fd_t;
#else
        typedef int poll_fd_t;
#endif

    public:
        server_manager_link(const char* server_manager, uint16_t port);
        ~server_manager_link() throw ();

    public:
        const configuration* config() { return &m_config; }
        poll_fd_t poll_fd() { return replicant_client_poll_fd(m_repl); }
        // true if there's a configuration to use
        // false if there's an error to report
        //
        // blocks if there's progress to be made toward getting a config
        bool ensure_configuration(replicant_returncode* status);
        bool get_unique_number(uint64_t &id);
        int64_t rpc(const char* func,
                    const char* data, size_t data_sz,
                    replicant_returncode* status,
                    char** output, size_t* output_sz);
        int64_t rpc_defended(const char* enter_func,
                             const char* enter_data, size_t enter_data_sz,
                             const char* exit_func,
                             const char* exit_data, size_t exit_data_sz,
                             replicant_returncode* status);
        int64_t backup(replicant_returncode* status,
                       char** output, size_t* output_sz);
        int64_t wait(const char* cond, uint64_t state,
                     replicant_returncode* status);
        int64_t loop(int timeout, replicant_returncode* status);
        int64_t wait(int64_t id, int timeout, replicant_returncode *status);
        std::string error_message() { return replicant_client_error_message(m_repl); }
        std::string error_location() { return replicant_client_error_location(m_repl); }

    private:
        server_manager_link(const server_manager_link&);
        server_manager_link& operator = (const server_manager_link&);

    private:
        bool prime_state_machine(replicant_returncode* status);
        bool process_new_configuration(replicant_returncode* status);
        void reset();

    private:
        replicant_client *m_repl;
        configuration m_config;
        int64_t m_id;
        replicant_returncode m_status;
        char* m_output;
        size_t m_output_sz;
};

#endif // weaver_common_server_manager_link_h_
