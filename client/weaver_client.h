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

#include "weaver/weaver_returncode.h"

#ifdef __cplusplus
extern "C"
{
#endif

struct property
{
    char *key;
    char *value;
};

struct edge
{
    char *handle;
    char *start_node;
    char *end_node;
    struct property *properties;
    size_t properties_sz;
};

struct node
{
    char *handle;
    struct property *properties;
    size_t properties_sz;
    struct edge *out_edges;
    size_t out_edges_sz;
    char **aliases;
    size_t aliases_sz;
};

struct weaver_client;

struct weaver_client*
weaver_client_create(const char *coordinator, uint16_t port, const char *config_file);

void
weaver_client_destroy(struct weaver_client *client);

enum weaver_client_returncode
weaver_client_begin_tx(struct weaver_client *client);

enum weaver_client_returncode
weaver_client_create_node(struct weaver_client *client,
                          char **handle,
                          int create_handle,
                          const char **aliases,
                          size_t aliases_sz);

enum weaver_client_returncode
weaver_client_create_edge(struct weaver_client *client,
                          char **handle,
                          int create_handle,
                          const char *node1, int handle_or_alias1,
                          const char *node2, int handle_or_alias2);

enum weaver_client_returncode
weaver_client_delete_node(struct weaver_client *client,
                          const char *node, int handle_or_alias);

enum weaver_client_returncode
weaver_client_delete_edge(struct weaver_client *client,
                          const char *edge,
                          const char *node, int handle_or_alias);

enum weaver_client_returncode
weaver_client_set_node_property(struct weaver_client *client,
                                const char *node, int handle_or_alias,
                                const struct property *prop);

enum weaver_client_returncode
weaver_client_set_edge_property(struct weaver_client *client,
                                const char *node, int handle_or_alias,
                                const char *edge,
                                const struct property *prop);

enum weaver_client_returncode
weaver_client_add_alias(struct weaver_client *client,
                        const char *alias, const char *node);

enum weaver_client_returncode
weaver_client_end_tx(struct weaver_client *client);

enum weaver_client_returncode
weaver_client_abort_tx(struct weaver_client *client);

enum weaver_client_returncode
weaver_client_get_node(struct weaver_client *client,
                       const char *node_handle,
                       struct node **n);

enum weaver_client_returncode
weaver_client_get_edge(struct weaver_client *client,
                       const char *edge_handle,
                       struct edge **e);


#ifdef __cplusplus
}
#endif

#endif
