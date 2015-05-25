/*
 * ===============================================================
 *    Description:  C client implementation.
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2014, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "client/weaver_client.h"
#include "client/client.h"

#define C_WRAP_EXCEPT(X) \
    cl::client *cl = reinterpret_cast<cl::client*>(_cl); \
    try { \
        X \
    } \
    catch (std::bad_alloc& ba) { \
        returncode = WEAVER_CLIENT_NOMEM; \
    } \
    catch (...) { \
        returncode = WEAVER_CLIENT_EXCEPTION; \
    } \

#ifdef __cplusplus
extern "C"
{
#endif

struct weaver_client*
weaver_client_create(const char *coordinator, uint16_t port, const char *config_file)
{
    try {
        return reinterpret_cast<weaver_client*>(new cl::client(coordinator, port, config_file));
    }
    catch (std::bad_alloc& ba) {
        return NULL;
    }
    catch (...) {
        return NULL;
    }
}

void
weaver_client_destroy(struct weaver_client *client)
{
    delete reinterpret_cast<cl::client*>(client);
}

void
pre_create_handle(std::string &handle, char **c_handle, int create_handle)
{
    if (!create_handle) {
        handle = *c_handle;
    }
}

void
convert_c_handle(const std::string &handle, char **c_handle)
{
    char *new_handle = (char*)malloc(handle.size()+1);
    strncpy(new_handle, handle.c_str(), handle.size());
    new_handle[handle.size()] = '\0';

    *c_handle = new_handle;
}

void
post_create_handle(const std::string &handle, char **c_handle, int create_handle)
{
    if (create_handle) {
        convert_c_handle(handle, c_handle);
    }
}

void
prepare_handle_or_alias(std::string &node, std::string &alias,
                        const char *_node, int handle_or_alias)
{
    if (_node) {
        if (handle_or_alias) {
            alias = _node;
        } else {
            node = _node;
        }
    }
}

weaver_client_returncode
weaver_client_begin_tx(struct weaver_client *_cl)
{
    weaver_client_returncode returncode;

    C_WRAP_EXCEPT(
        returncode = cl->begin_tx();
    );

    return returncode;
}

weaver_client_returncode
weaver_client_create_node(struct weaver_client *_cl,
                          char **_handle,
                          int create_handle,
                          const char **_aliases,
                          size_t aliases_sz)
{
    weaver_client_returncode returncode;
    std::string handle;
    std::vector<std::string> aliases;
    pre_create_handle(handle, _handle, create_handle);
    for (size_t i = 0; i < aliases_sz; i++) {
        aliases.emplace_back(_aliases[i]);
    }

    C_WRAP_EXCEPT(
        returncode = cl->create_node(handle, aliases);
    )

    if (returncode == WEAVER_CLIENT_SUCCESS) {
        post_create_handle(handle, _handle, create_handle);
    }

    return returncode;
}

weaver_client_returncode
weaver_client_create_edge(struct weaver_client *_cl,
                          char **_handle,
                          int create_handle,
                          const char *_node1, int handle_or_alias1,
                          const char *_node2, int handle_or_alias2)
{
    weaver_client_returncode returncode;
    std::string handle;
    pre_create_handle(handle, _handle, create_handle);

    std::string node1, alias1;
    prepare_handle_or_alias(node1, alias1, _node1, handle_or_alias1);

    std::string node2, alias2;
    prepare_handle_or_alias(node2, alias2, _node2, handle_or_alias2);

    C_WRAP_EXCEPT(
        returncode = cl->create_edge(handle, node1, alias1, node2, alias2);
    )

    if (returncode == WEAVER_CLIENT_SUCCESS) {
        post_create_handle(handle, _handle, create_handle);
    }

    return returncode;
}

weaver_client_returncode
weaver_client_delete_node(struct weaver_client *_cl,
                          const char *_node, int handle_or_alias)
{
    weaver_client_returncode returncode;
    std::string node, alias;
    prepare_handle_or_alias(node, alias, _node, handle_or_alias);

    C_WRAP_EXCEPT(
        returncode = cl->delete_node(node, alias);
    );

    return returncode;
}

weaver_client_returncode
weaver_client_delete_edge(struct weaver_client *_cl,
                          const char *edge,
                          const char *_node, int handle_or_alias)
{
    weaver_client_returncode returncode;
    std::string node, alias;
    prepare_handle_or_alias(node, alias, _node, handle_or_alias);

    C_WRAP_EXCEPT(
        returncode = cl->delete_edge(edge, node, alias);
    );

    return returncode;
}

weaver_client_returncode
weaver_client_set_node_property(struct weaver_client *_cl,
                                const char *_node, int handle_or_alias,
                                const struct property *prop)
{
    weaver_client_returncode returncode;
    std::string node, alias;
    prepare_handle_or_alias(node, alias, _node, handle_or_alias);

    C_WRAP_EXCEPT(
        returncode = cl->set_node_property(node, alias, prop->key, prop->value);
    );

    return returncode;
}

weaver_client_returncode
weaver_client_set_edge_property(struct weaver_client *_cl,
                                const char *_node, int handle_or_alias,
                                const char *edge,
                                const struct property *prop)
{
    weaver_client_returncode returncode;
    std::string node, alias;
    prepare_handle_or_alias(node, alias, _node, handle_or_alias);

    C_WRAP_EXCEPT(
        returncode = cl->set_edge_property(node, alias, edge, prop->key, prop->value);
    );

    return returncode;
}

weaver_client_returncode
weaver_client_add_alias(struct weaver_client *_cl,
                        const char *alias, const char *node)
{
    weaver_client_returncode returncode;

    C_WRAP_EXCEPT(
        returncode = cl->add_alias(alias, node);
    );

    return returncode;
}

weaver_client_returncode
weaver_client_end_tx(struct weaver_client *_cl)
{
    weaver_client_returncode returncode;

    C_WRAP_EXCEPT(
        returncode = cl->end_tx();
    );

    return returncode;
}

weaver_client_returncode
weaver_client_abort_tx(struct weaver_client *_cl)
{
    weaver_client_returncode returncode;

    C_WRAP_EXCEPT(
        returncode = cl->abort_tx();
    );

    return returncode;
}

void
convert_c_properties(const std::vector<std::shared_ptr<cl::property>> &props,
                     struct property **c_props, size_t *props_sz)
{
    *props_sz = props.size();
    if (props.size() > 0) {
        *c_props = (struct property*)malloc(sizeof(struct property) * props.size());
    }

    for (size_t i = 0; i < props.size(); i++) {
        struct property &c_p = (*c_props)[i];
        const cl::property &p = *props[i];
        convert_c_handle(p.key, &c_p.key);
        convert_c_handle(p.value, &c_p.value);
    }
}

void
convert_c_edge(const cl::edge &edge,
               struct edge *c_edge)
{
    convert_c_handle(edge.handle, &c_edge->handle);
    convert_c_handle(edge.start_node, &c_edge->start_node);
    convert_c_handle(edge.end_node, &c_edge->end_node);
    convert_c_properties(edge.properties, &c_edge->properties, &c_edge->properties_sz);
}

void
convert_c_node(const cl::node &n, struct node *c_n)
{
    convert_c_handle(n.handle, &c_n->handle);
    convert_c_properties(n.properties, &c_n->properties, &c_n->properties_sz);

    c_n->out_edges_sz = n.out_edges.size();
    if (c_n->out_edges_sz > 0) {
        c_n->out_edges = (struct edge*)malloc(sizeof(struct edge) * c_n->out_edges_sz);
        size_t i = 0;
        for (const auto &p: n.out_edges) {
            convert_c_edge(p.second, c_n->out_edges+i);
            i++;
        }
    }

    c_n->aliases_sz = n.aliases.size();
    if (c_n->aliases_sz > 0) {
        c_n->aliases = (char**)malloc(sizeof(char*) * c_n->aliases_sz);
        size_t i = 0;
        for (const std::string &alias: n.aliases) {
            convert_c_handle(alias, c_n->aliases + i);
            i++;
        }
    }
}

weaver_client_returncode
weaver_client_get_node(struct weaver_client *_cl,
                       const char *node_handle,
                       struct node **n)
{
    weaver_client_returncode returncode;
    std::vector<std::pair<std::string, node_prog::node_get_params>> arg_vec;
    node_prog::node_get_params arg_params, ret_params;
    arg_params.props = true;
    arg_params.edges = true;
    arg_params.aliases = true;
    arg_vec.emplace_back(std::make_pair(node_handle, arg_params));

    C_WRAP_EXCEPT(
        returncode = cl->node_get_program(arg_vec, ret_params);
    );

    if (returncode == WEAVER_CLIENT_SUCCESS) {
        *n = (struct node*)malloc(sizeof(struct node));
        convert_c_node(ret_params.node, *n);
    }

    return returncode;
}

weaver_client_returncode
weaver_client_get_edge(struct weaver_client *_cl,
                       const char *edge_handle,
                       struct edge **e)
{
    weaver_client_returncode returncode;
    std::vector<std::pair<std::string, node_prog::edge_get_params>> arg_vec;
    node_prog::edge_get_params arg_params, ret_params;
    arg_vec.emplace_back(std::make_pair(edge_handle, arg_params));

    C_WRAP_EXCEPT(
        returncode = cl->edge_get_program(arg_vec, ret_params);
    );

    if (returncode == WEAVER_CLIENT_SUCCESS) {
        *e = (struct edge*)malloc(sizeof(struct edge));
        convert_c_edge(ret_params.response_edges.front(), *e);
    }

    return returncode;
}

enum weaver_client_returncode
weaver_client_destroy_handles(char *handles,
                              size_t handles_sz)
{
    if (handles_sz > 0) {
        free(handles);
    }

    return WEAVER_CLIENT_SUCCESS;
}

enum weaver_client_returncode
weaver_client_destroy_properties(struct property *props,
                                 size_t properties_sz)
{
    if (properties_sz > 0) {
        for (size_t i = 0; i < properties_sz; i++) {
            weaver_client_destroy_handles(props[i].key, 1);
            weaver_client_destroy_handles(props[i].value, 1);
        }
        free(props);
    }

    return WEAVER_CLIENT_SUCCESS;
}

enum weaver_client_returncode
weaver_client_destroy_edges(struct edge *edges,
                            size_t edges_sz)
{
    if (edges_sz > 0) {
        for (size_t i = 0; i < edges_sz; i++) {
            weaver_client_destroy_handles(edges[i].handle, 1);
            weaver_client_destroy_handles(edges[i].start_node, 1);
            weaver_client_destroy_handles(edges[i].end_node, 1);
            weaver_client_destroy_properties(edges[i].properties, edges[i].properties_sz);
        }
        free(edges);
    }

    return WEAVER_CLIENT_SUCCESS;
}

enum weaver_client_returncode
weaver_client_destroy_nodes(struct node *nodes,
                            size_t nodes_sz)
{
    if (nodes_sz > 0) {
        for (size_t i = 0; i < nodes_sz; i++) {
            weaver_client_destroy_handles(nodes[i].handle, 1);
            weaver_client_destroy_properties(nodes[i].properties, nodes[i].properties_sz);
            weaver_client_destroy_edges(nodes[i].out_edges, nodes[i].out_edges_sz);

            if (nodes[i].aliases_sz > 0) {
                for (size_t j = 0; j < nodes[i].aliases_sz; j++) {
                    weaver_client_destroy_handles(nodes[i].aliases[j], 1);
                }
            }
            free(nodes[i].aliases);
        }
        free(nodes);
    }

    return WEAVER_CLIENT_SUCCESS;
}

#ifdef __cplusplus
}
#endif
