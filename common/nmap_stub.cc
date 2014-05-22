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
#include "common/nmap_stub.h"

using nmap::nmap_stub;

void
nmap_stub :: put_mappings(std::unordered_map<uint64_t, uint64_t> &pairs_to_add)
{
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

        op_id = cl.put(space, (const char*)&entry.first, sizeof(int64_t), attrs_to_add+put_idx, 1, put_status+put_idx);
        assert(op_id >= 0);
        assert(opid_to_idx.find(op_id) == opid_to_idx.end());
        opid_to_idx[op_id] = put_idx;

        put_idx++;
        
        if (put_idx % 2000 == 0) {
            WDEBUG << "completed " << put_idx << " puts\n";
        }
    }

    int64_t loop_id;
    hyperdex_client_returncode loop_status;
    // call loop once for every put
    for (int64_t i = 0; i < num_pairs; i++) {
        loop_id = cl.loop(-1, &loop_status);
        assert(loop_id >= 0);
        assert(opid_to_idx.find(loop_id) != opid_to_idx.end());
        int64_t &loop_idx = opid_to_idx[loop_id];
        assert(loop_idx >= 0);

        if (loop_status != HYPERDEX_CLIENT_SUCCESS || put_status[loop_idx] != HYPERDEX_CLIENT_SUCCESS) {
            WDEBUG << "bad put for node at idx " << loop_idx
                   << ", put status: " << put_status[loop_idx]
                   << ", loop status: " << loop_status << std::endl;
            WDEBUG << "error message: " << cl.error_message() << std::endl;
            WDEBUG << "error loc: " << cl.error_location() << std::endl;
        }
        loop_idx = -1;

        if (i % 2000 == 0) {
            WDEBUG << "completed " << i << " put loops\n";
        }
    }

    free(attrs_to_add);
}

std::vector<std::pair<uint64_t, uint64_t>>
nmap_stub :: get_mappings(std::unordered_set<uint64_t> &toGet)
{
    class async_get
    {
        public:
            uint64_t key;
            int64_t op_id;
            hyperdex_client_returncode status;
            const hyperdex_client_attribute *attr;
            size_t attr_size;

            async_get() { status = (hyperdex_client_returncode)0; }
    };

    int64_t num_nodes = toGet.size();
    std::vector<async_get> results(num_nodes);
    std::unordered_map<int64_t, int64_t> opid_to_idx;
    opid_to_idx.reserve(num_nodes);

    auto next_node = toGet.begin();
    for (int64_t i = 0; i < num_nodes; i++) {
        results[i].key = *next_node;
        next_node++;

        results[i].op_id = cl.get(space, (char*)&(results[i].key), sizeof(int64_t), 
            &(results[i].status), &(results[i].attr), &(results[i].attr_size));
        assert(results[i].op_id >= 0);
        assert(opid_to_idx.find(results[i].op_id) == opid_to_idx.end());
        opid_to_idx[results[i].op_id] = i;

        if (i % 2000 == 0) {
            WDEBUG << "completed " << i << " gets\n";
        }
    }

    int64_t loop_id;
    hyperdex_client_returncode loop_status;
    uint64_t *val;
    std::vector<std::pair<uint64_t, uint64_t>> mappings;
    mappings.reserve(num_nodes);
    // call loop once for every get
    for (int64_t i = 0; i < num_nodes; i++) {
        loop_id = cl.loop(-1, &loop_status);
        assert(loop_id >= 0);
        assert(opid_to_idx.find(loop_id) != opid_to_idx.end());
        int64_t &loop_idx = opid_to_idx[loop_id];
        assert(loop_idx >= 0);

        if (loop_status != HYPERDEX_CLIENT_SUCCESS || results[loop_idx].status != HYPERDEX_CLIENT_SUCCESS) {
            WDEBUG << "bad get for node at idx " << loop_idx
                   << ", get status: " << results[loop_idx].status
                   << ", loop status: " << loop_status << std::endl;
        }

        if (results[loop_idx].attr_size == 0) {
            WDEBUG << "Key " << results[loop_idx].key << " did not exist in hyperdex" << std::endl;
        } else {
            assert(results[loop_idx].attr_size == 1);
            val = (uint64_t*)results[loop_idx].attr->value;
            mappings.emplace_back(std::make_pair(results[loop_idx].key, *val));
        }
        hyperdex_client_destroy_attrs(results[loop_idx].attr, results[loop_idx].attr_size);

        loop_idx = -1;

        if (i % 2000 == 0) {
            WDEBUG << "completed " << i << " get loops\n";
        }
    }

    return mappings;
}

void
nmap_stub :: del_mappings(std::vector<uint64_t> &toDel)
{
    int numNodes = toDel.size();
    std::vector<int64_t> results(numNodes);
    hyperdex_client_returncode get_status;

    for (int i = 0; i < numNodes; i++) {
        results[i] = cl.del(space, (char *) &(toDel[i]), sizeof(uint64_t), &get_status);
        if (results[i] < 0)
        {
            WDEBUG << "\"del\" returned " << results[i] << " with status " << get_status << std::endl;
            return;
        }
    }

    hyperdex_client_returncode loop_status;
    int64_t loop_id;
    // call loop once for every get
    for (int i = 0; i < numNodes; i++) {
        loop_id = cl.loop(-1, &loop_status);
        if (loop_id < 0) {
            WDEBUG << "get \"loop\" returned " << loop_id << " with status " << loop_status << std::endl;
            return;
        }

        // double check this ID exists
        bool found = false;
        for (int i = 0; i < numNodes; i++) {
            found = found || (results[i] == loop_id);
        }
        assert(found);
    }
}

// TODO
void
nmap_stub :: clean_up_space()
{
    //cl.rm_space(space);
}
