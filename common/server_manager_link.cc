/*
 * ===============================================================
 *    Description:  Implementation of server_manager_link.
 *
 *        Created:  2014-02-10 14:00:40
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

#include <e/endian.h>

#define weaver_debug_
// Weaver
#include "common/server_manager_link.h"
#include "common/weaver_constants.h"
#include "common/passert.h"

server_manager_link :: server_manager_link(const char* server_manager, uint16_t port)
    : m_repl(replicant_client_create(server_manager, port))
    , m_config()
    , m_id(-1)
    , m_status(REPLICANT_GARBAGE)
    , m_output(NULL)
    , m_output_sz(0)
{
    if (!m_repl) {
        WDEBUG << "bad m_repl, errno=" << strerror(errno) << std::endl;
    }
}

server_manager_link :: ~server_manager_link() throw ()
{
    reset();
    replicant_client_destroy(m_repl);
}

bool
server_manager_link :: ensure_configuration(replicant_returncode* status)
{
    if (!prime_state_machine(status))
    {
        return false;
    }

    PASSERT(m_id >= 0);
    int timeout = m_config.cluster() == 0 ? -1 : 0;
    int64_t lid = replicant_client_wait(m_repl, m_id, timeout, status);

    if (lid < 0)
    {
        return *status == REPLICANT_TIMEOUT && timeout == 0;
    }

    PASSERT(lid == m_id);
    return process_new_configuration(status);
}

bool
server_manager_link :: get_unique_number(uint64_t &unique_num)
{
    replicant_returncode status;
    int64_t rid = replicant_client_generate_unique_number(m_repl, &status, &unique_num);

    if (rid < 0) {
        WDEBUG << "could not get replicant id, error in replicant_client_generate_unique_number call" << std::endl;
        WDEBUG << error_message() << " @ "
               << error_location() << std::endl;
        return false;
    }

    while (true) {
        replicant_returncode lrc = REPLICANT_GARBAGE;
        int64_t lid = loop(-1, &lrc);

        if (lid < 0) {
            WDEBUG << "could not get replicant id, error in replicant_client_generate_unique_number call" << std::endl;
            WDEBUG << error_message() << " @ "
                   << error_location() << std::endl;
            return false;
        } else if (lid == rid) {
            break;
        } else {
            WDEBUG << "replicant_client_loop returned unexpected id=" << lid << ", expected=" << rid << std::endl;
        }
    }

    if (status != REPLICANT_SUCCESS) {
        WDEBUG << "could not get replicant id: "
               << replicant_returncode_to_string(status) << std::endl;
        WDEBUG << error_message() << " @ "
               << error_location() << std::endl;
        return false;
    }

    return true;
}

int64_t
server_manager_link :: rpc(const char* func,
                        const char* data, size_t data_sz,
                        replicant_returncode* status,
                        char** output, size_t* output_sz)
{
    return replicant_client_call(m_repl, "weaver", func, data, data_sz,
                                 REPLICANT_CALL_ROBUST, status, output, output_sz);
}

int64_t
server_manager_link :: rpc_defended(const char* enter_func,
                                    const char* enter_data, size_t enter_data_sz,
                                    const char* exit_func,
                                    const char* exit_data, size_t exit_data_sz,
                                    replicant_returncode* status)
{
    return replicant_client_defended_call(m_repl, "weaver", enter_func, enter_data, enter_data_sz,
                                          exit_func, exit_data, exit_data_sz, status);
}

int64_t
server_manager_link :: backup(replicant_returncode* status,
                           char** output, size_t* output_sz)
{
    return replicant_client_backup_object(m_repl, "weaver", status, output, output_sz);
}

int64_t
server_manager_link :: wait(const char* cond, uint64_t state,
                            replicant_returncode* status)
{
    return replicant_client_cond_wait(m_repl, "weaver", cond, state, status, NULL, NULL);
}

int64_t
server_manager_link :: loop(int timeout, replicant_returncode* status)
{
    if (!prime_state_machine(status))
    {
        return -1;
    }

    int64_t lid = replicant_client_loop(m_repl, timeout, status);

    if (lid == m_id)
    {
        return process_new_configuration(status) ? INT64_MAX : -1;
    }
    else
    {
        return lid;
    }
}

int64_t
server_manager_link :: wait(int64_t id, int timeout, replicant_returncode* status)
{
    if (!prime_state_machine(status))
    {
        return -1;
    }

    if (id == INT64_MAX)
    {
        id = m_id;
    }

    int64_t lid = replicant_client_wait(m_repl, id, timeout, status);

    if (lid == m_id)
    {
        return process_new_configuration(status) ? INT64_MAX : -1;
    }
    else
    {
        return lid;
    }
}

bool
server_manager_link :: prime_state_machine(replicant_returncode* status)
{
    if (m_id >= 0)
    {
        return true;
    }

    m_id = replicant_client_cond_wait(m_repl, "weaver", "config", m_config.version() + 1, &m_status, &m_output, &m_output_sz);
    *status = m_status;

    if (m_id >= 0)
    {
        return true;
    }
    else
    {
        reset();
        return false;
    }
}

bool
server_manager_link :: process_new_configuration(replicant_returncode* status)
{
    m_id = -1;

    if (m_status != REPLICANT_SUCCESS)
    {
        *status = m_status;
        reset();
        return false;
    }

    e::unpacker up(m_output, m_output_sz);
    configuration new_config;
    up = up >> new_config;
    reset();

    if (up.error())
    {
        *status = REPLICANT_SERVER_ERROR;
        return false;
    }

    m_config = new_config;
    return true;
}

void
server_manager_link :: reset()
{
    if (m_output)
    {
        free(m_output);
    }

    m_id = -1;
    m_status = REPLICANT_GARBAGE;
    m_output = NULL;
    m_output_sz = 0;
}
