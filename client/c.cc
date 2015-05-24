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

void
pre_create_handle(std::string &handle, char **c_handle, int create_handle)
{
    if (!create_handle) {
        handle = *c_handle;
    }
}

void
post_create_handle(std::string &handle, char **c_handle, int create_handle)
{
    if (create_handle) {
        char *new_handle = (char*)malloc(handle.size()+1);
        strncpy(new_handle, handle.c_str(), handle.size());
        new_handle[handle.size()] = '\0';

        *c_handle = new_handle;
    }
}

void
prepare_handle_or_alias(std::string &node, std::string &alias,
                        const char *_node, int handle_or_alias)
{
    if (handle_or_alias) {
        alias = _node;
    } else {
        node = _node;
    }
}

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
                                const char *key, const char *value)
{
    weaver_client_returncode returncode;
    std::string node, alias;
    prepare_handle_or_alias(node, alias, _node, handle_or_alias);

    C_WRAP_EXCEPT(
        returncode = cl->set_node_property(node, alias, key, value);
    );

    return returncode;
}

weaver_client_returncode
weaver_client_set_edge_property(struct weaver_client *_cl,
                                const char *_node, int handle_or_alias,
                                const char *edge,
                                const char *key, const char *value)
{
    weaver_client_returncode returncode;
    std::string node, alias;
    prepare_handle_or_alias(node, alias, _node, handle_or_alias);

    C_WRAP_EXCEPT(
        returncode = cl->set_edge_property(node, alias, edge, key, value);
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
        cl->end_tx();
    );

    return returncode;
}

weaver_client_returncode
weaver_client_abort_tx(struct weaver_client *_cl)
{
    weaver_client_returncode returncode;

    C_WRAP_EXCEPT(
        cl->abort_tx();
    );

    return returncode;
}

#ifdef __cplusplus
}
#endif
