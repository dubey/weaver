/*
 * ===============================================================
 *    Description:  Replicant shim methods implementation.
 *
 *        Created:  2014-02-08 18:38:59
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *                  Robert Escriva, escriva@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

// Most of the following code has been 'borrowed' from
// Robert Escriva's HyperDex coordinator.
// see https://github.com/rescrv/HyperDex for the original code.

// C++
#include <new>

// STL
#include <string>

// Weaver
#include "common/server_manager_returncode.h"
#include "common/ids.h"
#include "common/serialization.h"
#include "coordinator/server_manager.h"
#include "coordinator/transitions.h"
#include "coordinator/util.h"

#define PROTECT_NULL \
    do \
    { \
        if (!obj) \
        { \
            fprintf(replicant_state_machine_log_stream(ctx), "cannot operate on NULL object\n"); \
            return generate_response(ctx, COORD_UNINITIALIZED); \
        } \
    } \
    while (0)

#define PROTECT_UNINITIALIZED \
    do \
    { \
        PROTECT_NULL; \
        coordinator::server_manager* c = static_cast<coordinator::server_manager*>(obj); \
        if (c->cluster() == 0) \
        { \
            fprintf(replicant_state_machine_log_stream(ctx), "cluster not initialized\n"); \
            return generate_response(ctx, COORD_UNINITIALIZED); \
        } \
    } \
    while (0)

#define CHECK_UNPACK(MSGTYPE) \
    do \
    { \
        if (up.error() || up.remain()) \
        { \
            fprintf(log, "received malformed \"" #MSGTYPE "\" message\n"); \
            return generate_response(ctx, COORD_MALFORMED); \
        } \
    } while (0)

extern "C"
{

void*
weaver_server_manager_create(struct replicant_state_machine_context* ctx)
{
    if (replicant_state_machine_condition_create(ctx, "config") < 0)
    {
        fprintf(replicant_state_machine_log_stream(ctx), "could not create condition \"config\"\n");
        return NULL;
    }

    if (replicant_state_machine_condition_create(ctx, "ack") < 0)
    {
        fprintf(replicant_state_machine_log_stream(ctx), "could not create condition \"ack\"\n");
        return NULL;
    }

    if (replicant_state_machine_condition_create(ctx, "stable") < 0)
    {
        fprintf(replicant_state_machine_log_stream(ctx), "could not create condition \"stable\"\n");
        return NULL;
    }

    coordinator::server_manager* c = new (std::nothrow) coordinator::server_manager();

    if (!c)
    {
        fprintf(replicant_state_machine_log_stream(ctx), "memory allocation failed\n");
    }

    fprintf(replicant_state_machine_log_stream(ctx), "created server manager object\n");
    return c;
}

void*
weaver_server_manager_recreate(struct replicant_state_machine_context* ctx,
                               const char* data, size_t data_sz)
{
    return coordinator::server_manager::recreate(ctx, data, data_sz);
}

void
weaver_server_manager_destroy(struct replicant_state_machine_context*, void* obj)
{
    if (obj)
    {
        delete static_cast<coordinator::server_manager*>(obj);
    }
}

void
weaver_server_manager_snapshot(struct replicant_state_machine_context* ctx,
                               void* obj, const char** data, size_t* data_sz)
{
    PROTECT_NULL;
    coordinator::server_manager* c = static_cast<coordinator::server_manager*>(obj);
    c->snapshot(ctx, data, data_sz);
}

void
weaver_server_manager_init(struct replicant_state_machine_context* ctx,
                          void* obj, const char* data, size_t data_sz)
{
    PROTECT_NULL;
    coordinator::server_manager* c = static_cast<coordinator::server_manager*>(obj);

    std::string id(data, data_sz);
    uint64_t cluster = strtoull(id.c_str(), NULL, 0);
    c->init(ctx, cluster);
}

void
weaver_server_manager_config_get(struct replicant_state_machine_context* ctx,
                                void* obj, const char*, size_t)
{
    PROTECT_UNINITIALIZED;
    coordinator::server_manager* c = static_cast<coordinator::server_manager*>(obj);
    c->config_get(ctx);
}

void
weaver_server_manager_config_ack(struct replicant_state_machine_context* ctx,
                                void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator::server_manager* c = static_cast<coordinator::server_manager*>(obj);
    server_id sid;
    uint64_t version;
    e::unpacker up(data, data_sz);
    up = up >> sid >> version;
    CHECK_UNPACK(config_ack);
    c->config_ack(ctx, sid, version);
}

void
weaver_server_manager_config_stable(struct replicant_state_machine_context* ctx,
                                   void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator::server_manager* c = static_cast<coordinator::server_manager*>(obj);
    server_id sid;
    uint64_t version;
    e::unpacker up(data, data_sz);
    up = up >> sid >> version;
    CHECK_UNPACK(config_stable);
    c->config_stable(ctx, sid, version);
}

void
weaver_server_manager_replid_get(struct replicant_state_machine_context* ctx,
                                   void* obj, const char*, size_t)
{
    PROTECT_UNINITIALIZED;
    coordinator::server_manager* c = static_cast<coordinator::server_manager*>(obj);
    c->replid_get(ctx);
}

void
weaver_server_manager_server_register(struct replicant_state_machine_context* ctx,
                                     void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator::server_manager* c = static_cast<coordinator::server_manager*>(obj);
    server_id sid;
    po6::net::location bind_to;
    int shard_or_vt;
    e::unpacker up(data, data_sz);
    up = up >> sid >> bind_to >> shard_or_vt;
    CHECK_UNPACK(server_register);
    c->server_register(ctx, sid, bind_to, shard_or_vt);
}

void
weaver_server_manager_server_online(struct replicant_state_machine_context* ctx,
                                   void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator::server_manager* c = static_cast<coordinator::server_manager*>(obj);
    server_id sid;
    po6::net::location bind_to;
    e::unpacker up(data, data_sz);
    up = up >> sid;

    if (!up.error() && !up.remain())
    {
        c->server_online(ctx, sid, NULL);
    }
    else
    {
        up = up >> bind_to;
        CHECK_UNPACK(server_online);
        c->server_online(ctx, sid, &bind_to);
    }
}

void
weaver_server_manager_server_offline(struct replicant_state_machine_context* ctx,
                                    void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator::server_manager* c = static_cast<coordinator::server_manager*>(obj);
    server_id sid;
    e::unpacker up(data, data_sz);
    up = up >> sid;
    CHECK_UNPACK(server_offline);
    c->server_offline(ctx, sid);
}

void
weaver_server_manager_server_shutdown(struct replicant_state_machine_context* ctx,
                                     void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator::server_manager* c = static_cast<coordinator::server_manager*>(obj);
    server_id sid;
    e::unpacker up(data, data_sz);
    up = up >> sid;
    CHECK_UNPACK(server_shutdown);
    c->server_shutdown(ctx, sid);
}

void
weaver_server_manager_server_kill(struct replicant_state_machine_context* ctx,
                                 void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator::server_manager* c = static_cast<coordinator::server_manager*>(obj);
    server_id sid;
    e::unpacker up(data, data_sz);
    up = up >> sid;
    CHECK_UNPACK(server_kill);
    c->server_kill(ctx, sid);
}

void
weaver_server_manager_server_forget(struct replicant_state_machine_context* ctx,
                                   void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator::server_manager* c = static_cast<coordinator::server_manager*>(obj);
    server_id sid;
    e::unpacker up(data, data_sz);
    up = up >> sid;
    CHECK_UNPACK(server_forget);
    c->server_forget(ctx, sid);
}

void
weaver_server_manager_server_suspect(struct replicant_state_machine_context* ctx,
                                    void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator::server_manager* c = static_cast<coordinator::server_manager*>(obj);
    server_id sid;
    e::unpacker up(data, data_sz);
    up = up >> sid;
    CHECK_UNPACK(server_suspect);
    c->server_suspect(ctx, sid);
}

void
weaver_server_manager_alarm(struct replicant_state_machine_context* ctx,
                           void* obj, const char*, size_t)
{
    PROTECT_UNINITIALIZED;
    coordinator::server_manager* c = static_cast<coordinator::server_manager*>(obj);
    c->alarm(ctx);
}

void
weaver_server_manager_debug_dump(struct replicant_state_machine_context* ctx,
                                void* obj, const char*, size_t)
{
    PROTECT_UNINITIALIZED;
    coordinator::server_manager* c = static_cast<coordinator::server_manager*>(obj);
    c->debug_dump(ctx);
}

} // extern "C"
