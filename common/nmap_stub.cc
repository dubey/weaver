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

nmap_stub :: nmap_stub()
    : cl(HyperdexCoordIpaddr, HyperdexCoordPort)
{ }

bool
nmap_stub :: put_mappings(std::unordered_map<node_id_t, uint64_t> &pairs_to_add)
{
    bool success = true;
    int num_pairs = pairs_to_add.size();
    hyperdex_client_attribute *attrs_to_add = (hyperdex_client_attribute *)malloc(num_pairs * sizeof(hyperdex_client_attribute));
    hyperdex_client_returncode put_status[num_pairs];
    std::unordered_map<int64_t, int64_t> opid_to_idx;
    opid_to_idx.reserve(num_pairs);

    int64_t put_idx = 0;
    int64_t op_id;
    for (auto &entry: pairs_to_add) {
        attrs_to_add[put_idx].attr = attrName;
        attrs_to_add[put_idx].value = (char*)&entry.second;
        attrs_to_add[put_idx].value_sz = sizeof(int64_t);
        attrs_to_add[put_idx].datatype = HYPERDATATYPE_INT64;

        do {
            op_id = cl.put(space, (const char*)&entry.first, sizeof(int64_t), attrs_to_add+put_idx, 1, put_status+put_idx);
        } while (op_id < 0);
        assert(opid_to_idx.find(op_id) == opid_to_idx.end());
        opid_to_idx[op_id] = put_idx;

        put_idx++;
        
        if (put_idx % 2000 == 0) {
            WDEBUG << "completed " << put_idx << " puts" << std::endl;
        }
    }

    int64_t loop_id;
    hyperdex_client_returncode loop_status;
    // call loop once for every put
    for (int64_t i = 0; i < num_pairs; i++) {
        do {
            loop_id = cl.loop(-1, &loop_status);
        } while (loop_id < 0);
        assert(opid_to_idx.find(loop_id) != opid_to_idx.end());
        int64_t &loop_idx = opid_to_idx[loop_id];
        assert(loop_idx >= 0);

        if (loop_status != HYPERDEX_CLIENT_SUCCESS || put_status[loop_idx] != HYPERDEX_CLIENT_SUCCESS) {
            WDEBUG << "bad put for node at idx " << loop_idx
                   << ", put status: " << put_status[loop_idx]
                   << ", loop status: " << loop_status << std::endl;
            WDEBUG << "error message: " << cl.error_message() << std::endl;
            WDEBUG << "error loc: " << cl.error_location() << std::endl;
            success = false;
        }
        loop_idx = -1;

        if (i > 0 && i % 2000 == 0) {
            WDEBUG << "completed " << i << " put loops" << std::endl;
        }
    }

    free(attrs_to_add);

    return success;
}

bool
nmap_stub :: put_client_mappings(std::unordered_map<std::string, node_id_t> &pairs_to_add)
{
    bool success = true;
    int num_pairs = pairs_to_add.size();
    hyperdex_client_attribute *attrs_to_add = (hyperdex_client_attribute *)malloc(num_pairs * sizeof(hyperdex_client_attribute));
    hyperdex_client_returncode put_status[num_pairs];
    std::unordered_map<int64_t, int64_t> opid_to_idx;
    opid_to_idx.reserve(num_pairs);

    int64_t put_idx = 0;
    int64_t op_id;
    for (auto &entry: pairs_to_add) {
        attrs_to_add[put_idx].attr = client_attr;
        attrs_to_add[put_idx].value = (char*)&entry.second;
        attrs_to_add[put_idx].value_sz = sizeof(int64_t);
        attrs_to_add[put_idx].datatype = HYPERDATATYPE_INT64;

        do {
            op_id = cl.put_if_not_exist(client_space, entry.first.c_str(), entry.first.size(), attrs_to_add+put_idx, 1, put_status+put_idx);
        } while (op_id < 0);
        assert(opid_to_idx.find(op_id) == opid_to_idx.end());
        opid_to_idx[op_id] = put_idx;

        put_idx++;
        
        if (put_idx % 2000 == 0) {
            WDEBUG << "completed " << put_idx << " puts" << std::endl;
        }
    }

    int64_t loop_id;
    hyperdex_client_returncode loop_status;
    // call loop once for every put
    for (int64_t i = 0; i < num_pairs; i++) {
        do {
            loop_id = cl.loop(-1, &loop_status);
        } while (loop_id < 0);
        assert(opid_to_idx.find(loop_id) != opid_to_idx.end());
        int64_t &loop_idx = opid_to_idx[loop_id];
        assert(loop_idx >= 0);

        if (loop_status != HYPERDEX_CLIENT_SUCCESS || put_status[loop_idx] != HYPERDEX_CLIENT_SUCCESS) {
            WDEBUG << "bad put for node at idx " << loop_idx
                   << ", put status: " << put_status[loop_idx]
                   << ", loop status: " << loop_status << std::endl;
            WDEBUG << "error message: " << cl.error_message() << std::endl;
            WDEBUG << "error loc: " << cl.error_location() << std::endl;
            success = false;
        }
        loop_idx = -1;

        if (i > 0 && i % 2000 == 0) {
            WDEBUG << "completed " << i << " put loops" << std::endl;
        }
    }

    free(attrs_to_add);

    return success;
}

std::vector<std::pair<node_id_t, uint64_t>>
nmap_stub :: get_mappings(std::unordered_set<node_id_t> &toGet)
{
    class async_get
    {
        public:
            node_id_t key;
            int64_t op_id;
            hyperdex_client_returncode status;
            const hyperdex_client_attribute *attr;
            size_t attr_size;

            async_get()
                : status(static_cast<hyperdex_client_returncode>(0))
                , attr(NULL)
                , attr_size(-1)
            { }
    };

    int64_t num_nodes = toGet.size();
    std::vector<async_get> results(num_nodes);
    std::unordered_map<int64_t, int64_t> opid_to_idx;
    opid_to_idx.reserve(num_nodes);

    auto next_node = toGet.begin();
    for (int64_t i = 0; i < num_nodes; i++) {
        results[i].key = *next_node;
        next_node++;

        do {
            results[i].op_id = cl.get(space, (char*)&(results[i].key), sizeof(int64_t), 
                &(results[i].status), &(results[i].attr), &(results[i].attr_size));
        } while (results[i].op_id < 0);
        assert(opid_to_idx.find(results[i].op_id) == opid_to_idx.end());
        opid_to_idx[results[i].op_id] = i;

        if (i > 0 && i % 2000 == 0) {
            WDEBUG << "completed " << i << " gets" << std::endl;
        }
    }

    int64_t loop_id;
    hyperdex_client_returncode loop_status;
    uint64_t *val;
    std::vector<std::pair<uint64_t, uint64_t>> mappings;
    mappings.reserve(num_nodes);
    // call loop once for every get
    for (int64_t i = 0; i < num_nodes; i++) {
        do {
            loop_id = cl.loop(-1, &loop_status);
        } while (loop_id < 0);
        assert(opid_to_idx.find(loop_id) != opid_to_idx.end());
        int64_t &loop_idx = opid_to_idx[loop_id];
        assert(loop_idx >= 0);

        if (loop_status != HYPERDEX_CLIENT_SUCCESS || results[loop_idx].status != HYPERDEX_CLIENT_SUCCESS) {
            if (results[loop_idx].status == HYPERDEX_CLIENT_NOTFOUND) {
                assert(results[loop_idx].attr_size == (size_t)-1);
                assert(results[loop_idx].attr == NULL);
            } else {
                WDEBUG << "bad get for node at idx " << loop_idx
                       << ", get status: " << results[loop_idx].status
                       << ", loop status: " << loop_status << std::endl;
                hyperdex_client_destroy_attrs(results[loop_idx].attr, results[loop_idx].attr_size);
            }
        } else {
            assert(results[loop_idx].attr_size == 1);
            val = (uint64_t*)results[loop_idx].attr->value;
            mappings.emplace_back(std::make_pair(results[loop_idx].key, *val));
            hyperdex_client_destroy_attrs(results[loop_idx].attr, results[loop_idx].attr_size);
        }

        loop_idx = -1;

        if (i > 0 && i % 2000 == 0) {
            WDEBUG << "completed " << i << " get loops" << std::endl;
        }
    }

    return mappings;
}

void
nmap_stub :: get_client_mappings(std::vector<std::string> &toGet, std::unordered_map<std::string, node_id_t> &client_map)
{
    class async_get
    {
        public:
            int64_t op_id;
            hyperdex_client_returncode status;
            const hyperdex_client_attribute *attr;
            size_t attr_size;

            async_get()
                : status(static_cast<hyperdex_client_returncode>(0))
                , attr(NULL)
                , attr_size(-1)
            { }
    };

    int64_t num_nodes = toGet.size();
    std::vector<async_get> results(num_nodes);
    std::unordered_map<int64_t, int64_t> opid_to_idx;
    opid_to_idx.reserve(num_nodes);

    for (int64_t i = 0; i < num_nodes; i++) {

        do {
            results[i].op_id = cl.get(client_space, toGet[i].c_str(), toGet[i].size(),
                &(results[i].status), &(results[i].attr), &(results[i].attr_size));
        } while (results[i].op_id < 0);
        assert(opid_to_idx.find(results[i].op_id) == opid_to_idx.end());
        opid_to_idx[results[i].op_id] = i;

        if (i > 0 && i % 2000 == 0) {
            WDEBUG << "completed " << i << " gets" << std::endl;
        }
    }

    int64_t loop_id;
    hyperdex_client_returncode loop_status;
    node_id_t *val;
    // call loop once for every get
    for (int64_t i = 0; i < num_nodes; i++) {
        do {
            loop_id = cl.loop(-1, &loop_status);
        } while (loop_id < 0);
        assert(opid_to_idx.find(loop_id) != opid_to_idx.end());
        int64_t &loop_idx = opid_to_idx[loop_id];
        assert(loop_idx >= 0);

        if (loop_status != HYPERDEX_CLIENT_SUCCESS || results[loop_idx].status != HYPERDEX_CLIENT_SUCCESS) {
            if (results[loop_idx].status == HYPERDEX_CLIENT_NOTFOUND) {
                assert(results[loop_idx].attr_size == (size_t)-1);
                assert(results[loop_idx].attr == NULL);
            } else {
                WDEBUG << "bad get for node at idx " << loop_idx
                       << ", get status: " << results[loop_idx].status
                       << ", loop status: " << loop_status << std::endl;
                hyperdex_client_destroy_attrs(results[loop_idx].attr, results[loop_idx].attr_size);
            }
        } else {
            assert(results[loop_idx].attr_size == 1);
            val = (node_id_t*)results[loop_idx].attr->value;
            client_map[toGet[loop_idx]] = *val;
            hyperdex_client_destroy_attrs(results[loop_idx].attr, results[loop_idx].attr_size);
        }

        loop_idx = -1;

        if (i > 0 && i % 2000 == 0) {
            WDEBUG << "completed " << i << " get loops" << std::endl;
        }
    }
}

bool
nmap_stub :: del_mappings(std::unordered_set<node_id_t> &toDel)
{
    bool success = true;
    int64_t num_nodes = toDel.size();
    node_id_t del_nodes[num_nodes];
    std::unordered_map<int64_t, int64_t> opid_to_idx;
    opid_to_idx.reserve(num_nodes);
    hyperdex_client_returncode del_status[num_nodes];
    int64_t del_id;

    auto todel_iter = toDel.begin();
    for (int64_t i = 0; i < num_nodes; i++) {
        assert(todel_iter != toDel.end());
        del_nodes[i] = *todel_iter;
        todel_iter++;
        do {
            del_id = cl.del(space, (char*)(del_nodes+i), sizeof(int64_t), del_status+i);
        } while (del_id < 0);
        assert(opid_to_idx.find(del_id) == opid_to_idx.end());
        opid_to_idx[del_id] = i;
    }

    int64_t loop_id;
    hyperdex_client_returncode loop_status;
    // call loop once for every get
    for (int64_t i = 0; i < num_nodes; i++) {
        do {
            loop_id = cl.loop(-1, &loop_status);
        } while(loop_id < 0);
        assert(opid_to_idx.find(loop_id) != opid_to_idx.end());
        int64_t &loop_idx = opid_to_idx[loop_id];
        assert(loop_idx >= 0);

        if (loop_status != HYPERDEX_CLIENT_SUCCESS
         || (del_status[loop_idx] != HYPERDEX_CLIENT_SUCCESS && del_status[loop_idx] != HYPERDEX_CLIENT_NOTFOUND)) {
            WDEBUG << "bad del for node at idx " << loop_idx
                   << ", node: " << del_nodes[loop_idx]
                   << ", del status: " << del_status[loop_idx]
                   << ", loop status: " << loop_status << std::endl;
            success = false;
        }

        loop_idx = -1;
    }

    return success;
}

bool
nmap_stub :: del_client_mappings(std::unordered_set<std::string> &toDel)
{
    bool success = true;
    int64_t num_nodes = toDel.size();
    int64_t del_nodes[num_nodes];
    std::unordered_map<int64_t, int64_t> opid_to_idx;
    opid_to_idx.reserve(num_nodes);
    hyperdex_client_returncode del_status[num_nodes];
    int64_t del_id;

    auto todel_iter = toDel.begin();
    for (int64_t i = 0; i < num_nodes; i++) {
        assert(todel_iter != toDel.end());
        do {
            del_id = cl.del(space, todel_iter->c_str(), todel_iter->size(), del_status+i);
        } while (del_id < 0);
        todel_iter++;
        assert(opid_to_idx.find(del_id) == opid_to_idx.end());
        opid_to_idx[del_id] = i;
    }

    int64_t loop_id;
    hyperdex_client_returncode loop_status;
    // call loop once for every get
    for (int64_t i = 0; i < num_nodes; i++) {
        do {
            loop_id = cl.loop(-1, &loop_status);
        } while(loop_id < 0);
        assert(opid_to_idx.find(loop_id) != opid_to_idx.end());
        int64_t &loop_idx = opid_to_idx[loop_id];
        assert(loop_idx >= 0);

        if (loop_status != HYPERDEX_CLIENT_SUCCESS
         || (del_status[loop_idx] != HYPERDEX_CLIENT_SUCCESS && del_status[loop_idx] != HYPERDEX_CLIENT_NOTFOUND)) {
            WDEBUG << "bad del for node at idx " << loop_idx
                   << ", node: " << del_nodes[loop_idx]
                   << ", del status: " << del_status[loop_idx]
                   << ", loop status: " << loop_status << std::endl;
            success = false;
        }

        loop_idx = -1;
    }

    return success;
}
