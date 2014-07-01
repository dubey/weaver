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

server_manager_link :: server_manager_link(const char* server_manager, uint16_t port)
    : m_repl(server_manager, port)
    , m_config()
    , m_state(NOTHING)
    , m_id(-1)
    , m_status(REPLICANT_GARBAGE)
    , m_output(NULL)
    , m_output_sz(0)
    , m_pending_ids()
{
}

server_manager_link :: ~server_manager_link() throw ()
{
    reset();
}

bool
server_manager_link :: force_configuration_fetch(replicant_returncode* status)
{
    if (m_id >= 0)
    {
        m_repl.kill(m_id);
    }

    if (m_output)
    {
        replicant_destroy_output(m_output, m_output_sz);
    }

    m_id = -1;
    m_state = WAITING_ON_BROADCAST;
    m_status = REPLICANT_SUCCESS;
    m_output = NULL;
    m_output_sz = 0;
    return begin_fetching_config(status);
}

bool
server_manager_link :: ensure_configuration(replicant_returncode* status)
{
    while (true)
    {
        if (!prime_state_machine(status))
        {
            return false;
        }

        assert(m_id >= 0);
        assert(m_state > NOTHING);
        int timeout = 0;

        switch (m_state)
        {
            case WAITING_ON_BROADCAST:
                timeout = 0;
                break;
            case FETCHING_CONFIG:
                timeout = -1;
                break;
            case NOTHING:
            default:
                abort();
        }

        int64_t lid = m_repl.loop(timeout, status);

        if (lid < 0 && *status == REPLICANT_TIMEOUT && timeout >= 0)
        {
            return true;
        }
        else if (lid < 0)
        {
            return false;
        }

        if (lid == m_id)
        {
            bool failed = false;

            if (handle_internal_callback(status, &failed))
            {
                return true;
            }

            if (failed)
            {
                return false;
            }
        }
        else
        {
            m_pending_ids.push_back(lid);
        }
    }
}

bool
server_manager_link :: get_replid(uint64_t &id)
{
    const char *buf = NULL;
    size_t zero = 0;
    replicant_returncode status;
    const char *output;
    size_t output_sz;
    int64_t rid = rpc("replid_get",
                      buf, zero,
                      &status,
                      &output,
                      &output_sz);

    if (rid < 0) {
        WDEBUG << "could not get replicant id" << std::endl;
        return false;
    }

    while (true) {
        replicant_returncode lrc = REPLICANT_GARBAGE;
        int64_t lid = loop(-1, &lrc);

        if (lid < 0) {
            e::error err = error();
            WDEBUG << "could not get replicant id: " << err.msg() << " @ " << err.loc() << std::endl;
            return false;
        } else if (lid == rid) {
            break;
        } else {
            WDEBUG << "retry get replicant id, " << "lid " << lid << ", rid " << rid << std::endl;
        }
    }

    if (status != REPLICANT_SUCCESS) {
        e::error err = error();
        WDEBUG << "could not get replicant id: " << err.msg() << " @ " << err.loc() << std::endl;
        return false;
    }

    if (output_sz >= sizeof(uint64_t)) {
        e::unpack64be(output, &id);
        return true;
    } else {
        WDEBUG << "could not get replicant id: server manager returned invalid message" << std::endl;
        return false;
    }

}

int64_t
server_manager_link :: rpc(const char* func,
                        const char* data, size_t data_sz,
                        replicant_returncode* status,
                        const char** output, size_t* output_sz)
{
    return m_repl.send("weaver", func, data, data_sz, status, output, output_sz);
}

int64_t
server_manager_link :: backup(replicant_returncode* status,
                           const char** output, size_t* output_sz)
{
    return m_repl.backup_object("weaver", status, output, output_sz);
}

int64_t
server_manager_link :: wait(const char* cond, uint64_t state,
                         replicant_returncode* status)
{
    return m_repl.wait("weaver", cond, state, status);
}

int64_t
server_manager_link :: loop(int timeout, replicant_returncode* status)
{
    if (!m_pending_ids.empty())
    {
        WDEBUG << "pending not empty, size " << m_pending_ids.size() << std::endl;
        int64_t ret = m_pending_ids.front();
        m_pending_ids.pop_front();
        return ret;
    }

    while (true)
    {
        if (!prime_state_machine(status))
        {
            return -1;
        }

        int64_t lid = m_repl.loop(timeout, status);

        if (lid == m_id)
        {
            bool failed = false;

            if (handle_internal_callback(status, &failed))
            {
                return INT64_MAX;
            }

            if (failed)
            {
                return -1;
            }
        }
        else
        {
            return lid;
        }
    }
}

bool
server_manager_link :: prime_state_machine(replicant_returncode* status)
{
    if (m_id >= 0)
    {
        return true;
    }

    if (m_state == NOTHING)
    {
        if (m_config.version() == 0)
        {
            m_state = WAITING_ON_BROADCAST;
            m_status = REPLICANT_SUCCESS;
            return begin_fetching_config(status);
        }
        else
        {
            return begin_waiting_on_broadcast(status);
        }
    }

    return true;
}

bool
server_manager_link :: handle_internal_callback(replicant_returncode* status, bool* failed)
{
    m_id = -1;

    if (m_status != REPLICANT_SUCCESS)
    {
        *status = m_status;
        *failed = true;
        reset();
        return false;
    }

    assert(m_state == WAITING_ON_BROADCAST ||
           m_state == FETCHING_CONFIG);

    if (m_state == WAITING_ON_BROADCAST)
    {
        if (!begin_fetching_config(status))
        {
            *failed = true;
            return false;
        }

        *failed = false;
        return false;
    }

    e::unpacker up(m_output, m_output_sz);
    configuration new_config;
    up = up >> new_config;
    reset();

    if (up.error())
    {
        *status = REPLICANT_MISBEHAVING_SERVER;
        *failed = true;
        return false;
    }

    if (m_config.cluster() != 0 &&
        m_config.cluster() != new_config.cluster())
    {
        *status = REPLICANT_MISBEHAVING_SERVER;
        *failed = true;
        return false;
    }

    m_config = new_config;
    return true;
}

bool
server_manager_link :: begin_waiting_on_broadcast(replicant_returncode* status)
{
    assert(m_id == -1);
    assert(m_state == NOTHING);
    assert(m_status == REPLICANT_GARBAGE);
    assert(m_output == NULL);
    assert(m_output_sz == 0);
    m_id = m_repl.wait("weaver", "config",
                       m_config.version() + 1, &m_status);
    m_state = WAITING_ON_BROADCAST;
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
server_manager_link :: begin_fetching_config(replicant_returncode* status)
{
    assert(m_id == -1);
    assert(m_state == WAITING_ON_BROADCAST);
    assert(m_status == REPLICANT_SUCCESS);
    assert(m_output == NULL);
    assert(m_output_sz == 0);
    m_id = m_repl.send("weaver", "config_get", "", 0,
                       &m_status, &m_output, &m_output_sz);
    m_state = FETCHING_CONFIG;
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

void
server_manager_link :: reset()
{
    if (m_output)
    {
        replicant_destroy_output(m_output, m_output_sz);
    }

    m_id = -1;
    m_state = NOTHING;
    m_status = REPLICANT_GARBAGE;
    m_output = NULL;
    m_output_sz = 0;
}
