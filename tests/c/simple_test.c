/*
 * ===============================================================
 *    Description:  Basic c client test.
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2015, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <weaver/weaver_client.h>

int main()
{
    struct weaver_client *client = weaver_client_create("127.0.0.1", 2002, NULL);
    enum weaver_client_returncode returncode;
    if (client == NULL) {
        printf("error creating client\n");
        return -1;
    }

    // tx 1
    weaver_client_begin_tx(client);

    char *ayush = "ayush";
    const char *alias = "alias";
    weaver_client_create_node(client, &ayush, 0, &alias, 1);

    struct property user;
    user.key = "type";
    user.value = "user";
    weaver_client_set_node_property(client, ayush, 0, &user);

    returncode = weaver_client_end_tx(client);
    assert(WEAVER_CLIENT_SUCCESS == returncode);


    // tx 2
    weaver_client_begin_tx(client);
    char *node2 = NULL;
    weaver_client_create_node(client, &node2, 1, NULL, 0);

    struct property post;
    post.key = "type";
    post.value = "post";
    weaver_client_set_node_property(client, node2, 0, &post);

    char *edge = NULL;
    weaver_client_create_edge(client, &edge, 1, ayush, 0, node2, 0);

    struct property owns;
    owns.key = "type";
    owns.value = "owner";
    weaver_client_set_edge_property(client, NULL, 0, edge, &owns);

    weaver_client_add_alias(client, "dubey", ayush);

    returncode = weaver_client_end_tx(client);
    assert(WEAVER_CLIENT_SUCCESS == returncode);


    // node prog
    struct node *n;
    returncode = weaver_client_get_node(client, "ayush", &n);
    assert(WEAVER_CLIENT_SUCCESS == returncode);
    size_t i;
    printf("printing node details\n");
    printf("handle=%s\n", n->handle);
    for (i = 0; i < n->properties_sz; i++) {
        printf("prop: %s=%s\n", n->properties[i].key, n->properties[i].value);
    }
    for (i = 0; i < n->out_edges_sz; i++) {
        printf("edge: handle=%s, endpoints=%s->%s, properties=", n->out_edges[i].handle,
                                                                 n->out_edges[i].start_node,
                                                                 n->out_edges[i].end_node);
        size_t j;
        for (j = 0; j < n->out_edges[i].properties_sz; j++) {
            if (j != 0) {
                printf(", ");
            }
            printf("%s=%s", n->out_edges[i].properties[j].key, n->out_edges[i].properties[j].value);
        }

        printf("\n");
    }
    for (i = 0; i < n->aliases_sz; i++) {
        printf("alias=%s\n", n->aliases[i]);
    }
    printf("done printing node details\n");

    // delete tx
    weaver_client_begin_tx(client);
    weaver_client_delete_edge(client, edge, NULL, 0);
    weaver_client_delete_node(client, node2, 0);
    weaver_client_delete_node(client, alias, 1);
    returncode = weaver_client_end_tx(client);
    assert(WEAVER_CLIENT_SUCCESS == returncode);

    weaver_client_destroy_handles(node2, 1);
    weaver_client_destroy_handles(edge, 1);
    weaver_client_destroy_nodes(n, 1);

    returncode = weaver_client_get_node(client, ayush, &n);
    assert(returncode == WEAVER_CLIENT_NOTFOUND);

    weaver_client_destroy(client);
    return 0;
}
