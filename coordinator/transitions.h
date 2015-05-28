/*
 * ===============================================================
 *    Description:  Replicant shim methods interface.
 *
 *        Created:  2014-02-10 12:10:01
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

#ifndef weaver_server_manager_transitions_h_
#define weaver_server_manager_transitions_h_
#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

/* Replicant */
#include <rsm.h>

void*
weaver_server_manager_create(struct rsm_context* ctx);

void*
weaver_server_manager_recreate(struct rsm_context* ctx,
                              const char* data, size_t data_sz);

int
weaver_server_manager_snapshot(struct rsm_context* ctx,
                              void* obj, char** data, size_t* sz);

#define TRANSITION(X) void \
    weaver_server_manager_ ## X(struct rsm_context* ctx, \
                               void* obj, const char* data, size_t data_sz)

TRANSITION(init);

TRANSITION(config_get);
TRANSITION(config_ack);
TRANSITION(config_stable);

TRANSITION(server_register);
TRANSITION(server_online);
TRANSITION(server_offline);
TRANSITION(server_shutdown);
TRANSITION(server_kill);
TRANSITION(server_forget);
TRANSITION(server_suspect);
TRANSITION(report_disconnect);

TRANSITION(debug_dump);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
#endif /* weaver_server_manager_transitions_h_ */
