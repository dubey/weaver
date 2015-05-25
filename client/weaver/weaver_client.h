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

// node and edge properties struct
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

// create a new weaver client object
// coordinator is the IP address of Weaver server manager process
// port is the port of Weaver server manager process process
// config_file is the path of weaver.yaml config file.  If NULL is passed, the config file at /usr/local/etc or /etc will be used
struct weaver_client*
weaver_client_create(const char *coordinator, uint16_t port, const char *config_file);

// destroy a weaver client
void
weaver_client_destroy(struct weaver_client *client);

// start a weaver transaction
enum weaver_client_returncode
weaver_client_begin_tx(struct weaver_client *client);

// create a weaver node
// *handle is the handle of the new node
// if create_handle is 0, we assume the client passes in a unique handle
// if create_handle is non zero, weaver will create a unique handle and store it in *handle.
// if weaver creates a handle, the allocated memory needs to be freed once done using weaver_client_destroy_handles
// aliases is an array of node aliases and aliases_sz is the size of the array.  if no aliases, aliases_sz should be 0
enum weaver_client_returncode
weaver_client_create_node(struct weaver_client *client,
                          char **handle,
                          int create_handle,
                          const char **aliases,
                          size_t aliases_sz);

// create a weaver directed edge node1 -> node2
// *handle is the handle of the new edge
// if create_handle is 0, we assume the client passes in a unique handle
// if create_handle is non zero, weaver will create a unique handle and store it in *handle.
// if weaver creates a handle, the allocated memory needs to be freed once done using weaver_client_destroy_handles
// node1 is the node handle or node alias of the first node
// handle_or_alias1 is 0 if node1 is node handle, otherwise node1 is an alias
// node2 args similar to node1
enum weaver_client_returncode
weaver_client_create_edge(struct weaver_client *client,
                          char **handle,
                          int create_handle,
                          const char *node1, int handle_or_alias1,
                          const char *node2, int handle_or_alias2);

// delete a weaver node
// node is the node handle or node alias of the node
// handle_or_alias is 0 if node is node handle, otherwise node is an alias
enum weaver_client_returncode
weaver_client_delete_node(struct weaver_client *client,
                          const char *node, int handle_or_alias);

// delete a weaver edge
// edge is the edge handle
// node is the node handle or node alias of the node that owns the edge
// passing in node makes the call faster by avoiding an extra lookup using the edge, but it is optional.
// if the client does not wish to pass the node, they should simply pass NULL
// handle_or_alias is 0 if node is node handle, otherwise node is an alias
enum weaver_client_returncode
weaver_client_delete_edge(struct weaver_client *client,
                          const char *edge,
                          const char *node, int handle_or_alias);

// set a property on a weaver node
// node is the node handle or node alias of the node
// handle_or_alias is 0 if node is node handle, otherwise node is an alias
// prop is the weaver property to be set on the node
enum weaver_client_returncode
weaver_client_set_node_property(struct weaver_client *client,
                                const char *node, int handle_or_alias,
                                const struct property *prop);

// set a property on a weaver edge
// node is the node handle or node alias of the node that owns the edge
// passing in node makes the call faster by avoiding an extra lookup using the edge, but it is optional.
// if the client does not wish to pass the node, they should simply pass NULL
// handle_or_alias is 0 if node is node handle, otherwise node is an alias
// edge is the edge handle
// prop is the weaver property to be set on the edge
enum weaver_client_returncode
weaver_client_set_edge_property(struct weaver_client *client,
                                const char *node, int handle_or_alias,
                                const char *edge,
                                const struct property *prop);

// add an alias to a node
enum weaver_client_returncode
weaver_client_add_alias(struct weaver_client *client,
                        const char *alias, const char *node);

// commit a weaver transaction
enum weaver_client_returncode
weaver_client_end_tx(struct weaver_client *client);

// abort a weaver transaction
enum weaver_client_returncode
weaver_client_abort_tx(struct weaver_client *client);

// get a weaver node
// node can either be a node_handle or a node alias
// if the call is successful, we allocate memory for the object and return it in *n
// the allocated memory needs to be freed once done using weaver_client_destroy_nodes
enum weaver_client_returncode
weaver_client_get_node(struct weaver_client *client,
                       const char *node,
                       struct node **n);

// get a weaver node
// edge is the edge handle
// if the call is successful, we allocate memory for the object and return it in *e
// the allocated memory needs to be freed once done using weaver_client_destroy_edges
enum weaver_client_returncode
weaver_client_get_edge(struct weaver_client *client,
                       const char *edge,
                       struct edge **e);

// free memory allocated for weaver handles
// handles is an array of handles of size handle_sz
enum weaver_client_returncode
weaver_client_destroy_handles(char *handles,
                              size_t handles_sz);

// free memory allocated for weaver properties
// properties is an array of properties of size properties_sz
enum weaver_client_returncode
weaver_client_destroy_properties(struct property *props,
                                 size_t properties_sz);

// free memory allocated for weaver edges
// edges is an array of edges of size edges_sz
enum weaver_client_returncode
weaver_client_destroy_edges(struct edge *edges,
                            size_t edges_sz);

// free memory allocated for weaver nodes
// nodes is an array of nodes of size nodes_sz
enum weaver_client_returncode
weaver_client_destroy_nodes(struct node *nodes,
                            size_t nodes_sz);

#ifdef __cplusplus
}
#endif

#endif
