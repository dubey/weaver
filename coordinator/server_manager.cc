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
#include <unordered_set>

// e
#include <e/endian.h>

// Replicant
#include <rsm.h>

// Weaver
#include "common/server_manager_returncode.h"
#include "coordinator/server_manager.h"
#include "coordinator/util.h"
#include "common/passert.h"

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
    , m_num_shards(0)
    , m_num_vts(0)
    , m_num_weaver(0)
    , m_servers()
    , m_config_ack_through(0)
    , m_config_ack_barrier()
    , m_config_stable_through(0)
    , m_config_stable_barrier()
    , m_latest_config()
{
    PASSERT(m_config_ack_through == m_config_ack_barrier.min_version());
    PASSERT(m_config_stable_through == m_config_stable_barrier.min_version());

    generate_cached_configuration(nullptr);
}

server_manager :: ~server_manager() throw ()
{
}

void
server_manager :: init(rsm_context* ctx, uint64_t token)
{
    if (m_cluster != 0)
    {
        rsm_log(ctx, "cannot initialize Weaver cluster with id %lu "
                     "because it is already initialized to %lu\n", token, m_cluster);
        // we lie to the client and pretend all is well
        return generate_response(ctx, COORD_SUCCESS);
    }

    rsm_log(ctx, "initializing Weaver cluster with id %lu\n", token);
    m_cluster = token;
    generate_next_configuration(ctx);
    return generate_response(ctx, COORD_SUCCESS);
}

void
server_manager :: server_register(rsm_context* ctx,
                                  const server_id& sid,
                                  const po6::net::location& bind_to,
                                  server::type_t type)
{
    server* srv = get_server(sid);

    if (srv)
    {
        std::string str(to_string(srv->bind_to));
        rsm_log(ctx, "cannot register server(%lu) because the id belongs to "
                     "server(%lu, %s)\n", sid.get(), srv->id.get(), str.c_str());
        return generate_response(ctx, COORD_DUPLICATE);
    }

    srv = new_server(sid);
    srv->state = server::ASSIGNED;
    srv->bind_to = bind_to;
    srv->type = type;
    srv->weaver_id = m_num_weaver++;

    switch (type) {
        case server::SHARD:
            srv->virtual_id = m_num_shards++;
            break;

        case server::VT:
            srv->virtual_id = m_num_vts++;
            break;

        default:
            break;
    }

    check_backup(ctx, srv);

    rsm_log(ctx, "registered server(%lu)\n", sid.get());
    generate_next_configuration(ctx);
    return generate_response(ctx, COORD_SUCCESS);
}

void
server_manager :: server_online(rsm_context* ctx,
                             const server_id& sid,
                             const po6::net::location* bind_to)
{
    server* srv = get_server(sid);

    if (!srv)
    {
        rsm_log(ctx, "cannot bring server(%lu) online because "
                     "the server doesn't exist\n", sid.get());
        return generate_response(ctx, COORD_NOT_FOUND);
    }

    if (srv->state != server::ASSIGNED &&
        srv->state != server::NOT_AVAILABLE &&
        srv->state != server::SHUTDOWN &&
        srv->state != server::AVAILABLE)
    {
        rsm_log(ctx, "cannot bring server(%lu) online because the server is "
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
                rsm_log(ctx, "cannot change server(%lu) to %s "
                             "because that address is in use by "
                             "server(%lu)\n", sid.get(), to.c_str(),
                             m_servers[i].id.get());
                return generate_response(ctx, COORD_DUPLICATE);
            }
        }

        rsm_log(ctx, "changing server(%lu)'s address from %s to %s\n",
                     sid.get(), from.c_str(), to.c_str());
        srv->bind_to = *bind_to;
        changed = true;
    }

    if (srv->state != server::AVAILABLE)
    {
        rsm_log(ctx, "changing server(%lu) from %s to %s\n",
                     sid.get(), server::to_string(srv->state),
                     server::to_string(server::AVAILABLE));
        srv->state = server::AVAILABLE;

        changed = true;
    }

    if (changed)
    {
        generate_next_configuration(ctx);
    }

    return generate_response(ctx, COORD_SUCCESS);
}

void
server_manager :: server_offline(rsm_context* ctx,
                              const server_id& sid)
{
    server* srv = get_server(sid);

    if (!srv)
    {
        rsm_log(ctx, "cannot bring server(%lu) offline because "
                     "the server doesn't exist\n", sid.get());
        return generate_response(ctx, COORD_NOT_FOUND);
    }

    if (srv->state != server::ASSIGNED &&
        srv->state != server::NOT_AVAILABLE &&
        srv->state != server::AVAILABLE &&
        srv->state != server::SHUTDOWN)
    {
        rsm_log(ctx, "cannot bring server(%lu) offline because the server is "
                     "%s\n", sid.get(), server::to_string(srv->state));
        return generate_response(ctx, COORD_NO_CAN_DO);
    }

    if (srv->state != server::NOT_AVAILABLE && srv->state != server::SHUTDOWN)
    {
        rsm_log(ctx, "changing server(%lu) from %s to %s\n",
                     sid.get(), server::to_string(srv->state),
                     server::to_string(server::NOT_AVAILABLE));
        srv->state = server::NOT_AVAILABLE;
        find_backup(ctx, srv->type, srv->virtual_id);
        generate_next_configuration(ctx);
    }

    return generate_response(ctx, COORD_SUCCESS);
}

void
server_manager :: server_shutdown(rsm_context* ctx,
                               const server_id& sid)
{
    server* srv = get_server(sid);

    if (!srv)
    {
        rsm_log(ctx, "cannot shutdown server(%lu) because "
                     "the server doesn't exist\n", sid.get());
        return generate_response(ctx, COORD_NOT_FOUND);
    }

    if (srv->state != server::ASSIGNED &&
        srv->state != server::NOT_AVAILABLE &&
        srv->state != server::AVAILABLE &&
        srv->state != server::SHUTDOWN)
    {
        rsm_log(ctx, "cannot shutdown server(%lu) because the server is "
                     "%s\n", sid.get(), server::to_string(srv->state));
        return generate_response(ctx, COORD_NO_CAN_DO);
    }

    if (srv->state == server::ASSIGNED && srv->state == server::AVAILABLE) {
        find_backup(ctx, srv->type, srv->virtual_id);
    }

    if (srv->state != server::SHUTDOWN)
    {
        rsm_log(ctx, "changing server(%lu) from %s to %s\n",
                     sid.get(), server::to_string(srv->state),
                     server::to_string(server::SHUTDOWN));
        srv->state = server::SHUTDOWN;
        generate_next_configuration(ctx);
    }

    return generate_response(ctx, COORD_SUCCESS);
}

void
server_manager :: server_kill(rsm_context* ctx,
                           const server_id& sid)
{
    server* srv = get_server(sid);

    if (!srv)
    {
        rsm_log(ctx, "cannot kill server(%lu) because "
                     "the server doesn't exist\n", sid.get());
        return generate_response(ctx, COORD_NOT_FOUND);
    }

    if (srv->state == server::ASSIGNED && srv->state == server::AVAILABLE) {
        find_backup(ctx, srv->type, srv->virtual_id);
    }

    if (srv->state != server::KILLED)
    {
        rsm_log(ctx, "changing server(%lu) from %s to %s\n",
                     sid.get(), server::to_string(srv->state),
                     server::to_string(server::KILLED));
        srv->state = server::KILLED;
        generate_next_configuration(ctx);
    }

    return generate_response(ctx, COORD_SUCCESS);
}

void
server_manager :: server_forget(rsm_context* ctx,
                             const server_id& sid)
{
    server* srv = get_server(sid);

    if (!srv)
    {
        rsm_log(ctx, "cannot forget server(%lu) because "
                     "the server doesn't exist\n", sid.get());
        return generate_response(ctx, COORD_NOT_FOUND);
    }

    if (srv->state == server::ASSIGNED && srv->state == server::AVAILABLE) {
        find_backup(ctx, srv->type, srv->virtual_id);
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
server_manager :: server_suspect(rsm_context* ctx,
                              const server_id& sid)
{
    server* srv = get_server(sid);

    if (!srv)
    {
        rsm_log(ctx, "cannot suspect server(%lu) because "
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
        rsm_log(ctx, "cannot suspect server(%lu) because the server is "
                     "%s\n", sid.get(), server::to_string(srv->state));
        return generate_response(ctx, COORD_NO_CAN_DO);
    }

    if (srv->state != server::NOT_AVAILABLE && srv->state != server::SHUTDOWN)
    {
        rsm_log(ctx, "changing server(%lu) from %s to %s because we suspect it failed\n",
                     sid.get(), server::to_string(srv->state),
                     server::to_string(server::NOT_AVAILABLE));
        srv->state = server::NOT_AVAILABLE;
        find_backup(ctx, srv->type, srv->virtual_id);
        generate_next_configuration(ctx);
    }

    return generate_response(ctx, COORD_SUCCESS);
}

void
server_manager :: report_disconnect(rsm_context *ctx,
                                    const server_id &sid,
                                    uint64_t version)
{
    if (m_version != version) {
        return;
    }

    return server_suspect(ctx, sid);
}

void
server_manager :: check_backup(rsm_context* ctx, server *new_srv)
{
    if (new_srv->type != server::BACKUP_SHARD && new_srv->type != server::BACKUP_VT) {
        return;
    }

    std::unordered_set<uint64_t> dead_servers;
    server::type_t server_type = (new_srv->type == server::BACKUP_VT)? server::VT : server::SHARD;

    for (const server &srv: m_servers) {
        if (srv.state != server::ASSIGNED
         && srv.state != server::AVAILABLE
         && srv.type == server_type) {
            dead_servers.emplace(srv.virtual_id);
        }
    }

    for (const server &srv: m_servers) {
        if ((srv.state == server::AVAILABLE || srv.state == server::ASSIGNED)
         && srv.type == server_type) {
            dead_servers.erase(srv.virtual_id);
        }
    }

    for (uint64_t vid: dead_servers) {
        rsm_log(ctx, "activating backup %lu for dead %s %lu\n",
                new_srv->weaver_id, server::to_string(server_type), vid);
        activate_backup(new_srv, server_type, vid);
        break;
    }
}

void
server_manager :: find_backup(rsm_context* ctx, server::type_t type, uint64_t vid)
{
    server::type_t target_type;

    switch (type) {
        case server::SHARD:
            target_type = server::BACKUP_SHARD;
            break;

        case server::VT:
            target_type = server::BACKUP_VT;
            break;

        default:
            rsm_log(ctx, "unexpected type: (int) %d, (string) %s\n",
                    type, server::to_string(type));
            return;
    }

    for (size_t i = 0; i < m_servers.size(); i++) {
        if (m_servers[i].type == target_type) {
            // found backup server
            rsm_log(ctx, "activating backup %lu for dead %s %lu\n",
                    m_servers[i].weaver_id, server::to_string(type), vid);
            activate_backup(&m_servers[i], type, vid);
            break;
        }
    }
}

void
server_manager :: activate_backup(server *backup, server::type_t type, uint64_t vid)
{
    if (type == server::VT) {
        PASSERT(backup->type == server::BACKUP_VT);
    } else {
        PASSERT(backup->type == server::BACKUP_SHARD);
    }

    backup->type = type;
    backup->virtual_id = vid;
}

void
server_manager :: config_get(rsm_context* ctx)
{
    PASSERT(m_latest_config.get());
    const char* output = reinterpret_cast<const char*>(m_latest_config->data());
    size_t output_sz = m_latest_config->size();
    rsm_set_output(ctx, output, output_sz);
}

void
server_manager :: config_ack(rsm_context* ctx,
                          const server_id& sid, uint64_t version)
{
    m_config_ack_barrier.pass(version, sid);
    check_ack_condition(ctx);
}

void
server_manager :: config_stable(rsm_context* ctx,
                             const server_id& sid, uint64_t version)
{
    m_config_stable_barrier.pass(version, sid);
    check_stable_condition(ctx);
}

void
server_manager :: debug_dump(rsm_context* ctx)
{
    rsm_log(ctx, "=== begin debug dump ===========================================================\n");
    rsm_log(ctx, "config ack through: %lu\n", m_config_ack_through);
    rsm_log(ctx, "config stable through: %lu\n", m_config_stable_through);
    rsm_log(ctx, "=== end debug dump =============================================================\n");
}

server_manager*
server_manager :: recreate(rsm_context* ctx,
                           const char* data, size_t data_sz)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
    std::auto_ptr<server_manager> c(new server_manager());
#pragma GCC diagnostic pop

    if (!c.get())
    {
        rsm_log(ctx, "memory allocation failed\n");
        return nullptr;
    }

    e::unpacker up(data, data_sz);
    up = up >> c->m_cluster >> c->m_counter >> c->m_version >> c->m_flags >> c->m_servers
            >> c->m_config_ack_through >> c->m_config_ack_barrier
            >> c->m_config_stable_through >> c->m_config_stable_barrier;

    if (up.error())
    {
        rsm_log(ctx, "unpacking failed\n");
        return nullptr;
    }

    c->generate_cached_configuration(ctx);
    return c.release();
}

int
server_manager :: snapshot(rsm_context* /*CANNOT USE ctx in snapshot*/,
                        char** data, size_t* data_sz)
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

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
    std::auto_ptr<e::buffer> buf(e::buffer::create(sz));
#pragma GCC diagnostic pop
    e::packer pa = buf->pack_at(0);
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

    return 0;
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

    return nullptr;
}

void
server_manager :: check_ack_condition(rsm_context* ctx)
{
    if (m_config_ack_through < m_config_ack_barrier.min_version())
    {
        rsm_log(ctx, "acked through version %lu\n", m_config_ack_barrier.min_version());
    }

    while (m_config_ack_through < m_config_ack_barrier.min_version())
    {
        rsm_cond_broadcast(ctx, "ack");
        ++m_config_ack_through;
    }
}

void
server_manager :: check_stable_condition(rsm_context* ctx)
{
    if (m_config_stable_through < m_config_stable_barrier.min_version())
    {
        rsm_log(ctx, "stable through version %lu\n", m_config_stable_barrier.min_version());
    }

    while (m_config_stable_through < m_config_stable_barrier.min_version())
    {
        rsm_cond_broadcast(ctx, "stable");
        ++m_config_stable_through;
    }
}

void
server_manager :: generate_next_configuration(rsm_context* ctx)
{
    ++m_version;
    rsm_log(ctx, "issuing new configuration version %lu\n", m_version);
    std::vector<server_id> sids;
    servers_in_configuration(&sids);
    m_config_ack_barrier.new_version(m_version, sids);
    m_config_stable_barrier.new_version(m_version, sids);
    check_ack_condition(ctx);
    check_stable_condition(ctx);
    generate_cached_configuration(ctx);

    rsm_cond_broadcast_data(ctx, "config", m_latest_config->cdata(), m_latest_config->size());
    rsm_log(ctx, "#servers in config=%lu\n", m_servers.size());
}

void
server_manager :: generate_cached_configuration(rsm_context*)
{
    m_latest_config.reset();
    size_t sz = 4 * sizeof(uint64_t);

    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        sz += pack_size(m_servers[i]);
    }

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
    std::auto_ptr<e::buffer> new_config(e::buffer::create(sz));
#pragma GCC diagnostic pop
    e::packer pa = new_config->pack_at(0);
    pa = pa << m_cluster << m_version << m_flags
            << uint64_t(m_servers.size());

    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        pa = pa << m_servers[i];
    }

    m_latest_config = new_config;
}

// always send state of all servers
void
server_manager :: servers_in_configuration(std::vector<server_id>* sids)
{
    for (std::vector<server>::iterator it = m_servers.begin(); it != m_servers.end(); it++) {
        sids->push_back(it->id);
    }
}
