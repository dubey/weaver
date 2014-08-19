/*
 * ===============================================================
 *    Description:  Implementation of node mapper Hyperdex stub.
 *
 *        Created:  2014-05-22 11:53:59
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#define weaver_debug_

#include <assert.h>
#include "common/weaver_constants.h"
#include "common/config_constants.h"
#include "common/nmap_stub.h"

using nmap::nmap_stub;

bool
nmap_stub :: put_mappings(std::unordered_map<node_handle_t, uint64_t> &pairs_to_add)
{
    int num_pairs = pairs_to_add.size();
    std::vector<hyper_func> funcs(num_pairs, &hyperdex::Client::put);
    std::vector<const char*> spaces(num_pairs, space);
    std::vector<const char*> keys(num_pairs);
    std::vector<size_t> key_szs(num_pairs);
    std::vector<hyperdex_client_attribute*> attrs(num_pairs);
    std::vector<size_t> num_attrs(num_pairs, 1);

    hyperdex_client_attribute *attrs_to_add = (hyperdex_client_attribute*)malloc(num_pairs * sizeof(hyperdex_client_attribute));

    int i = 0;
    for (auto &entry: pairs_to_add) {
        attrs[i] = attrs_to_add + i;
        attrs[i]->attr = attrName;
        attrs[i]->value = (char*)&entry.second;
        attrs[i]->value_sz = sizeof(int64_t);
        attrs[i]->datatype = HYPERDATATYPE_INT64;
        keys[i] = entry.first.c_str();
        key_szs[i] = entry.first.size();
        i++;
    }

    bool success = multiple_call(funcs, spaces, keys, key_szs, attrs, num_attrs);

    free(attrs_to_add);

    return success;
}


std::vector<std::pair<node_handle_t, uint64_t>>
nmap_stub :: get_mappings(std::unordered_set<node_handle_t> &toGet)
{
    int64_t num_nodes = toGet.size();
    std::vector<const char*> spaces(num_nodes, space);
    std::vector<const char*> keys(num_nodes);
    std::vector<size_t> key_szs(num_nodes);
    std::vector<const hyperdex_client_attribute**> attrs(num_nodes, NULL);
    std::vector<size_t*> num_attrs(num_nodes, NULL);

    const hyperdex_client_attribute *attr_array[num_nodes];
    size_t num_attrs_array[num_nodes];

    int i = 0;
    for (auto &n: toGet) {
        keys[i] = n.c_str();
        key_szs[i] = n.size();
        attrs[i] = attr_array + i;
        num_attrs[i] = num_attrs_array + i;

        i++;
    }

    bool success = multiple_get(spaces, keys, key_szs, attrs, num_attrs);
    UNUSED(success);

    uint64_t *val;
    std::vector<std::pair<node_handle_t, uint64_t>> mappings;
    mappings.reserve(num_nodes);
    for (int64_t i = 0; i < num_nodes; i++) {
        if (*num_attrs[i] == 1) {
            val = (uint64_t*)attr_array[i]->value;
            mappings.emplace_back(std::make_pair(node_handle_t(keys[i]), *val));
            hyperdex_client_destroy_attrs(attr_array[i], 1);
        } else if (*num_attrs[i] > 0) {
            WDEBUG << "bad num attributes " << *num_attrs[i] << std::endl;
            hyperdex_client_destroy_attrs(attr_array[i], *num_attrs[i]);
        }
    }

    return mappings;
}


bool
nmap_stub :: del_mappings(std::unordered_set<node_handle_t> &toDel)
{
    int64_t num_nodes = toDel.size();
    std::vector<const char*> spaces(num_nodes, space);
    std::vector<const char*> keys(num_nodes);
    std::vector<size_t> key_szs(num_nodes);

    int i = 0;
    for (auto &n: toDel) {
        keys[i] = n.c_str();
        key_szs[i] = n.size();

        i++;
    }

    return multiple_del(spaces, keys, key_szs);
}
