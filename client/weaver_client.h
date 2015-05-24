/*
 * ===============================================================
 *    Description:  Weaver C client
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2015, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_client_weaver_client_h
#define weaver_client_weaver_client_h

#include <stddef.h>
#include <stdint.h>

#include "client/weaver_returncode.h"

#ifdef __cplusplus
extern "C"
{
#endif

struct property
{
    const char *key;
    const char *value;
};

struct edge
{
    const char *handle;
    const char *start_node;
    const char *end_node;
    const property *properties;
    size_t properties_sz;
};

struct node
{
    const char *handle;
    const property *properties;
    size_t properties_sz;
    const edge *out_edges;
    size_t out_edges_sz;
    const char **aliases;
    size_t aliases_sz;
};

struct weaver_client;

struct weaver_client*
weaver_client_create(const char *coordinator, uint16_t port, const char *config_file=NULL);

void
weaver_client_destroy(struct weaver_client *client);

weaver_client_returncode
weaver_client_begin_tx(struct weaver_client *client);

weaver_client_returncode
weaver_client_create_node(struct weaver_client *client,
                          char **handle,
                          int create_handle,
                          const char **aliases,
                          size_t aliases_sz);

weaver_client_returncode
weaver_client_create_edge(struct weaver_client *client,
                          char **handle,
                          int create_handle,
                          const char *node1, int handle_or_alias1,
                          const char *node2, int handle_or_alias2);

weaver_client_returncode
weaver_client_delete_node(struct weaver_client *client,
                          const char *node, int handle_or_alias);

weaver_client_returncode
weaver_client_delete_edge(struct weaver_client *client,
                          const char *edge,
                          const char *node, int handle_or_alias);

weaver_client_returncode
weaver_client_set_node_property(struct weaver_client *client,
                                const char *node, int handle_or_alias,
                                const char *key, const char *value);

weaver_client_returncode
weaver_client_set_edge_property(struct weaver_client *client,
                                const char *node, int handle_or_alias,
                                const char *edge,
                                const char *key, const char *value);

weaver_client_returncode
weaver_client_add_alias(struct weaver_client *client,
                        const char *alias, const char *node);

weaver_client_returncode
weaver_client_end_tx(struct weaver_client *client);

weaver_client_returncode
weaver_client_abort_tx(struct weaver_client *client);


#ifdef __cplusplus
}
#endif

#endif
