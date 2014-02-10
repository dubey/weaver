/*
 * ===============================================================
 *    Description:  Server manager implementation.
 *
 *        Created:  2014-02-08 13:58:10
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

// STL
#include <algorithm>
#include <sstream>

// e
#include <e/endian.h>

// Replicant
#include <replicant_state_machine.h>

// Weaver
#include "common/server_manager_returncode.h"
#include "coordinator/server_manager.h"
#include "coordinator/util.h"

#define ALARM_INTERVAL 30

// XXX does this matter for Weaver?
// ASSUME:  I'm assuming only one server ever changes state at a time for a
//          given transition.  If you violate this assumption, fixup
//          converge_intent.

namespace
{

std::string
to_string(const po6::net::location& loc)
{
    std::ostringstream oss;
    oss << loc;
    return oss.str();
}

} // namespace

using coordinator::server_manager;

server_manager :: server_manager()
    : m_cluster(0)
    , m_counter(1)
    , m_version(0)
    , m_flags(0)
    , m_servers()
    , m_config_ack_through(0)
    , m_config_ack_barrier()
    , m_config_stable_through(0)
    , m_config_stable_barrier()
    , m_latest_config()
{
    assert(m_config_ack_through == m_config_ack_barrier.min_version());
    assert(m_config_stable_through == m_config_stable_barrier.min_version());
}

server_manager :: ~server_manager() throw ()
{
}

void
server_manager :: init(replicant_state_machine_context* ctx, uint64_t token)
{
    FILE* log = replicant_state_machine_log_stream(ctx);

    if (m_cluster != 0)
    {
        fprintf(log, "cannot initialize Weaver cluster with id %lu "
                     "because it is already initialized to %lu\n", token, m_cluster);
        // we lie to the client and pretend all is well
        return generate_response(ctx, COORD_SUCCESS);
    }

    replicant_state_machine_alarm(ctx, "alarm", ALARM_INTERVAL);
    fprintf(log, "initializing Weaver cluster with id %lu\n", token);
    m_cluster = token;
    generate_next_configuration(ctx);
    return generate_response(ctx, COORD_SUCCESS);
}

void
server_manager :: server_register(replicant_state_machine_context* ctx,
                               const server_id& sid,
                               const po6::net::location& bind_to)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    server* srv = get_server(sid);

    if (srv)
    {
        std::string str(to_string(srv->bind_to));
        fprintf(log, "cannot register server(%lu) because the id belongs to "
                     "server(%lu, %s)\n", sid.get(), srv->id.get(), str.c_str());
        return generate_response(ctx, COORD_DUPLICATE);
    }

    srv = new_server(sid);
    srv->state = server::ASSIGNED;
    srv->bind_to = bind_to;
    fprintf(log, "registered server(%lu)\n", sid.get());
    generate_next_configuration(ctx);
    return generate_response(ctx, COORD_SUCCESS);
}

void
server_manager :: server_online(replicant_state_machine_context* ctx,
                             const server_id& sid,
                             const po6::net::location* bind_to)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    server* srv = get_server(sid);

    if (!srv)
    {
        fprintf(log, "cannot bring server(%lu) online because "
                     "the server doesn't exist\n", sid.get());
        return generate_response(ctx, COORD_NOT_FOUND);
    }

    if (srv->state != server::ASSIGNED &&
        srv->state != server::NOT_AVAILABLE &&
        srv->state != server::SHUTDOWN &&
        srv->state != server::AVAILABLE)
    {
        fprintf(log, "cannot bring server(%lu) online because the server is "
                     "%s\n", sid.get(), server::to_string(srv->state));
        return generate_response(ctx, COORD_NO_CAN_DO);
    }

    bool changed = false;

    if (bind_to && srv->bind_to != *bind_to)
    {
        std::string from(to_string(srv->bind_to));
        std::string to(to_string(*bind_to));

        for (size_t i = 0; i < m_servers.size(); ++i)
        {
            if (m_servers[i].id != sid &&
                m_servers[i].bind_to == *bind_to)
            {
                fprintf(log, "cannot change server(%lu) to %s "
                             "because that address is in use by "
                             "server(%lu)\n", sid.get(), to.c_str(),
                             m_servers[i].id.get());
                return generate_response(ctx, COORD_DUPLICATE);
            }
        }

        fprintf(log, "changing server(%lu)'s address from %s to %s\n",
                     sid.get(), from.c_str(), to.c_str());
        srv->bind_to = *bind_to;
        changed = true;
    }

    if (srv->state != server::AVAILABLE)
    {
        fprintf(log, "changing server(%lu) from %s to %s\n",
                     sid.get(), server::to_string(srv->state),
                     server::to_string(server::AVAILABLE));
        srv->state = server::AVAILABLE;

        changed = true;
    }

    if (changed)
    {
        generate_next_configuration(ctx);
    }

    char buf[sizeof(uint64_t)];
    e::pack64be(sid.get(), buf);
    uint64_t client = replicant_state_machine_get_client(ctx);
    replicant_state_machine_suspect(ctx, client, "server_suspect",  buf, sizeof(uint64_t));
    return generate_response(ctx, COORD_SUCCESS);
}

void
server_manager :: server_offline(replicant_state_machine_context* ctx,
                              const server_id& sid)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    server* srv = get_server(sid);

    if (!srv)
    {
        fprintf(log, "cannot bring server(%lu) offline because "
                     "the server doesn't exist\n", sid.get());
        return generate_response(ctx, COORD_NOT_FOUND);
    }

    if (srv->state != server::ASSIGNED &&
        srv->state != server::NOT_AVAILABLE &&
        srv->state != server::AVAILABLE &&
        srv->state != server::SHUTDOWN)
    {
        fprintf(log, "cannot bring server(%lu) offline because the server is "
                     "%s\n", sid.get(), server::to_string(srv->state));
        return generate_response(ctx, COORD_NO_CAN_DO);
    }

    if (srv->state != server::NOT_AVAILABLE && srv->state != server::SHUTDOWN)
    {
        fprintf(log, "changing server(%lu) from %s to %s\n",
                     sid.get(), server::to_string(srv->state),
                     server::to_string(server::NOT_AVAILABLE));
        srv->state = server::NOT_AVAILABLE;
        generate_next_configuration(ctx);
    }

    return generate_response(ctx, COORD_SUCCESS);
}

void
server_manager :: server_shutdown(replicant_state_machine_context* ctx,
                               const server_id& sid)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    server* srv = get_server(sid);

    if (!srv)
    {
        fprintf(log, "cannot shutdown server(%lu) because "
                     "the server doesn't exist\n", sid.get());
        return generate_response(ctx, COORD_NOT_FOUND);
    }

    if (srv->state != server::ASSIGNED &&
        srv->state != server::NOT_AVAILABLE &&
        srv->state != server::AVAILABLE &&
        srv->state != server::SHUTDOWN)
    {
        fprintf(log, "cannot shutdown server(%lu) because the server is "
                     "%s\n", sid.get(), server::to_string(srv->state));
        return generate_response(ctx, COORD_NO_CAN_DO);
    }

    if (srv->state != server::SHUTDOWN)
    {
        fprintf(log, "changing server(%lu) from %s to %s\n",
                     sid.get(), server::to_string(srv->state),
                     server::to_string(server::SHUTDOWN));
        srv->state = server::SHUTDOWN;
        generate_next_configuration(ctx);
    }

    return generate_response(ctx, COORD_SUCCESS);
}

void
server_manager :: server_kill(replicant_state_machine_context* ctx,
                           const server_id& sid)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    server* srv = get_server(sid);

    if (!srv)
    {
        fprintf(log, "cannot kill server(%lu) because "
                     "the server doesn't exist\n", sid.get());
        return generate_response(ctx, COORD_NOT_FOUND);
    }

    if (srv->state != server::KILLED)
    {
        fprintf(log, "changing server(%lu) from %s to %s\n",
                     sid.get(), server::to_string(srv->state),
                     server::to_string(server::KILLED));
        srv->state = server::KILLED;
        generate_next_configuration(ctx);
    }

    return generate_response(ctx, COORD_SUCCESS);
}

void
server_manager :: server_forget(replicant_state_machine_context* ctx,
                             const server_id& sid)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    server* srv = get_server(sid);

    if (!srv)
    {
        fprintf(log, "cannot forget server(%lu) because "
                     "the server doesn't exist\n", sid.get());
        return generate_response(ctx, COORD_NOT_FOUND);
    }

    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        if (m_servers[i].id == sid)
        {
            std::swap(m_servers[i], m_servers[m_servers.size() - 1]);
            m_servers.pop_back();
        }
    }

    std::stable_sort(m_servers.begin(), m_servers.end());
    generate_next_configuration(ctx);
    return generate_response(ctx, COORD_SUCCESS);
}

void
server_manager :: server_suspect(replicant_state_machine_context* ctx,
                              const server_id& sid)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    server* srv = get_server(sid);

    if (!srv)
    {
        fprintf(log, "cannot suspect server(%lu) because "
                     "the server doesn't exist\n", sid.get());
        return generate_response(ctx, COORD_NOT_FOUND);
    }

    if (srv->state == server::SHUTDOWN)
    {
        return generate_response(ctx, COORD_SUCCESS);
    }

    if (srv->state != server::ASSIGNED &&
        srv->state != server::NOT_AVAILABLE &&
        srv->state != server::AVAILABLE)
    {
        fprintf(log, "cannot suspect server(%lu) because the server is "
                     "%s\n", sid.get(), server::to_string(srv->state));
        return generate_response(ctx, COORD_NO_CAN_DO);
    }

    if (srv->state != server::NOT_AVAILABLE && srv->state != server::SHUTDOWN)
    {
        fprintf(log, "changing server(%lu) from %s to %s because we suspect it failed\n",
                     sid.get(), server::to_string(srv->state),
                     server::to_string(server::NOT_AVAILABLE));
        srv->state = server::NOT_AVAILABLE;
        generate_next_configuration(ctx);
    }

    return generate_response(ctx, COORD_SUCCESS);
}

void
server_manager :: config_get(replicant_state_machine_context* ctx)
{
    assert(m_cluster != 0 && m_version != 0);
    assert(m_latest_config.get());
    const char* output = reinterpret_cast<const char*>(m_latest_config->data());
    size_t output_sz = m_latest_config->size();
    replicant_state_machine_set_response(ctx, output, output_sz);
}

void
server_manager :: config_ack(replicant_state_machine_context* ctx,
                          const server_id& sid, uint64_t version)
{
    m_config_ack_barrier.pass(version, sid);
    check_ack_condition(ctx);
}

void
server_manager :: config_stable(replicant_state_machine_context* ctx,
                             const server_id& sid, uint64_t version)
{
    m_config_stable_barrier.pass(version, sid);
    check_stable_condition(ctx);
}

void
server_manager :: alarm(replicant_state_machine_context* ctx)
{
    replicant_state_machine_alarm(ctx, "alarm", ALARM_INTERVAL);
}

void
server_manager :: debug_dump(replicant_state_machine_context* ctx)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    fprintf(log, "=== begin debug dump ===========================================================\n");
    fprintf(log, "config ack through: %lu\n", m_config_ack_through);
    fprintf(log, "config stable through: %lu\n", m_config_stable_through);
    fprintf(log, "=== end debug dump =============================================================\n");
}

server_manager*
server_manager :: recreate(replicant_state_machine_context* ctx,
                           const char* data, size_t data_sz)
{
    std::auto_ptr<server_manager> c(new server_manager());

    if (!c.get())
    {
        fprintf(replicant_state_machine_log_stream(ctx), "memory allocation failed\n");
        return NULL;
    }

    e::unpacker up(data, data_sz);
    up = up >> c->m_cluster >> c->m_counter >> c->m_version >> c->m_flags >> c->m_servers
            >> c->m_config_ack_through >> c->m_config_ack_barrier
            >> c->m_config_stable_through >> c->m_config_stable_barrier;

    if (up.error())
    {
        fprintf(replicant_state_machine_log_stream(ctx), "unpacking failed\n");
        return NULL;
    }

    c->generate_cached_configuration(ctx);
    replicant_state_machine_alarm(ctx, "alarm", ALARM_INTERVAL);
    return c.release();
}

void
server_manager :: snapshot(replicant_state_machine_context* /*ctx*/,
                        const char** data, size_t* data_sz)
{
    size_t sz = sizeof(m_cluster)
              + sizeof(m_counter)
              + sizeof(m_version)
              + sizeof(m_flags)
              + pack_size(m_servers)
              + sizeof(m_config_ack_through)
              + pack_size(m_config_ack_barrier)
              + sizeof(m_config_stable_through)
              + pack_size(m_config_stable_barrier);

    std::auto_ptr<e::buffer> buf(e::buffer::create(sz));
    e::buffer::packer pa = buf->pack_at(0);
    pa = pa << m_cluster << m_counter << m_version << m_flags << m_servers
            << m_config_ack_through << m_config_ack_barrier
            << m_config_stable_through << m_config_stable_barrier;

    char* ptr = static_cast<char*>(malloc(buf->size()));
    *data = ptr;
    *data_sz = buf->size();

    if (*data)
    {
        memmove(ptr, buf->data(), buf->size());
    }
}

server*
server_manager :: new_server(const server_id& sid)
{
    size_t idx = m_servers.size();
    m_servers.push_back(server(sid));

    for (; idx > 0; --idx)
    {
        if (m_servers[idx - 1].id < m_servers[idx].id)
        {
            break;
        }

        std::swap(m_servers[idx - 1], m_servers[idx]);
    }

    return &m_servers[idx];
}

server*
server_manager :: get_server(const server_id& sid)
{
    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        if (m_servers[i].id == sid)
        {
            return &m_servers[i];
        }
    }

    return NULL;
}

void
server_manager :: check_ack_condition(replicant_state_machine_context* ctx)
{
    if (m_config_ack_through < m_config_ack_barrier.min_version())
    {
        FILE* log = replicant_state_machine_log_stream(ctx);
        fprintf(log, "acked through version %lu\n", m_config_ack_barrier.min_version());
    }

    while (m_config_ack_through < m_config_ack_barrier.min_version())
    {
        replicant_state_machine_condition_broadcast(ctx, "ack", &m_config_ack_through);
    }
}

void
server_manager :: check_stable_condition(replicant_state_machine_context* ctx)
{
    if (m_config_stable_through < m_config_stable_barrier.min_version())
    {
        FILE* log = replicant_state_machine_log_stream(ctx);
        fprintf(log, "stable through version %lu\n", m_config_stable_barrier.min_version());
    }

    while (m_config_stable_through < m_config_stable_barrier.min_version())
    {
        replicant_state_machine_condition_broadcast(ctx, "stable", &m_config_stable_through);
    }
}

void
server_manager :: generate_next_configuration(replicant_state_machine_context* ctx)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    uint64_t cond_state;

    if (replicant_state_machine_condition_broadcast(ctx, "config", &cond_state) < 0)
    {
        fprintf(log, "could not broadcast on \"config\" condition\n");
    }

    ++m_version;
    fprintf(log, "issuing new configuration version %lu\n", m_version);
    assert(cond_state == m_version);
    std::vector<server_id> sids;
    servers_in_configuration(&sids);
    m_config_ack_barrier.new_version(m_version, sids);
    m_config_stable_barrier.new_version(m_version, sids);
    check_ack_condition(ctx);
    check_stable_condition(ctx);
    generate_cached_configuration(ctx);
}

void
server_manager :: generate_cached_configuration(replicant_state_machine_context*)
{
    m_latest_config.reset();
    size_t sz = 4 * sizeof(uint64_t);

    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        sz += pack_size(m_servers[i]);
    }

    std::auto_ptr<e::buffer> new_config(e::buffer::create(sz));
    e::buffer::packer pa = new_config->pack_at(0);
    pa = pa << m_cluster << m_version << m_flags
            << uint64_t(m_servers.size());

    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        pa = pa << m_servers[i];
    }

    m_latest_config = new_config;
}

// XXX why not just return m_servers?
void
server_manager :: servers_in_configuration(std::vector<server_id>* sids)
{
    for (std::vector<server>::iterator it = m_servers.begin();
         it != m_servers.end(); it++) {
        if (it->state == server::AVAILABLE) {
            sids->push_back(it->id);
        }
    }
    /*
    for (std::map<std::string, std::tr1::shared_ptr<space> >::iterator it = m_spaces.begin();
            it != m_spaces.end(); ++it)
    {
        space& s(*it->second);

        for (size_t i = 0; i < s.subspaces.size(); ++i)
        {
            subspace& ss(s.subspaces[i]);

            for (size_t j = 0; j < ss.regions.size(); ++j)
            {
                region& reg(ss.regions[j]);

                for (size_t k = 0; k < reg.replicas.size(); ++k)
                {
                    sids->push_back(reg.replicas[k].si);
                }
            }
        }
    }

    for (size_t i = 0; i < m_transfers.size(); ++i)
    {
        sids->push_back(m_transfers[i].src);
        sids->push_back(m_transfers[i].dst);
    }

    std::sort(sids->begin(), sids->end());
    std::vector<server_id>::iterator sit;
    sit = std::unique(sids->begin(), sids->end());
    sids->resize(sit - sids->begin());
    */
}
