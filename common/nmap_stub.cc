/*
 * ===============================================================
 *    Description:  Node mapper stub implementation.
 *
 *        Created:  2014-04-01 13:54:51
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013-2014, Cornell University, see the LICENSE
 *                     file for licensing agreement
 * ===============================================================
 */

#define weaver_debug_
#include "common/weaver_constants.h"
#include "common/nmap_stub.h"

using nmap::nmap_stub;

nmap_stub :: nmap_stub() : cl(HYPERDEX_COORD_IPADDR, HYPERDEX_COORD_PORT) { }

void
nmap_stub :: put_mappings(std::unordered_map<uint64_t, uint64_t> &pairs_to_add)
{
    int numPairs = pairs_to_add.size();
    int i;
    hyperdex_client_attribute *attrs_to_add = (hyperdex_client_attribute *) malloc(numPairs * sizeof(hyperdex_client_attribute));

    i = 0;
    uint32_t num_div10 = numPairs / 10;
    if (num_div10 < 1) {
        num_div10 = 1;
    }

    int num_loops = 0;
    for (auto &entry: pairs_to_add) {
        attrs_to_add[i].attr = attrName;
        attrs_to_add[i].value = (char *) &entry.second;
        attrs_to_add[i].value_sz = sizeof(int64_t);
        attrs_to_add[i].datatype = HYPERDATATYPE_INT64;

        hyperdex_client_returncode put_status = HYPERDEX_CLIENT_INTERRUPTED;
        int64_t op_id = -1;
        //uint64_t tries = 0;
        while (op_id < 0 && put_status == HYPERDEX_CLIENT_INTERRUPTED) {
            op_id = cl.put(space, (const char *) &entry.first, sizeof(int64_t), &(attrs_to_add[i]), 1, &put_status);
            num_loops++;
            if (put_status == HYPERDEX_CLIENT_INTERRUPTED) {
                //WDEBUG << "trying to put in nmap " << (tries++) << std::endl;
            }
        }
        if (op_id < 0) {
            WDEBUG << "\"put\" returned " << op_id << " with status " << put_status << std::endl;
            free(attrs_to_add);
            return;
        } else {
            //WDEBUG << "put " << entry.first << ":" << entry.second << std::endl;
        }
        i++;
    }

    hyperdex_client_returncode loop_status = HYPERDEX_CLIENT_INTERRUPTED;
    int64_t loop_id = -1;
    // call loop once for every put
    //for (i = 0; i < numPairs; i++) {
    for (i = 0; i < num_loops; i++) {
        while (loop_id < 0 && loop_status == HYPERDEX_CLIENT_INTERRUPTED) {
            loop_id = cl.loop(-1, &loop_status);
        }
        if (loop_id < 0) {
            WDEBUG << "put \"loop\" returned " << loop_id << " with status " << loop_status << std::endl;
            free(attrs_to_add);
            return;
        }
    }
    free(attrs_to_add);
}

std::vector<std::pair<uint64_t, uint64_t>>
nmap_stub :: get_mappings(std::unordered_set<uint64_t> &toGet)
{
    int numNodes = toGet.size();
    struct async_get {
        uint64_t key;
        int64_t op_id;
        hyperdex_client_returncode get_status;
        const hyperdex_client_attribute * attr;
        size_t attr_size;
    };
    std::vector<struct async_get> results(numNodes);
    auto nextHandle = toGet.begin();
    for (int i = 0; i < numNodes; i++) {
        results[i].key = *nextHandle;
        results[i].op_id = -1;
        results[i].get_status = HYPERDEX_CLIENT_INTERRUPTED;
        nextHandle++;
    }

    int num_loops = 0;
    for (int i = 0; i < numNodes; i++) {
        while (results[i].op_id < 0 && results[i].get_status == HYPERDEX_CLIENT_INTERRUPTED) {
            results[i].op_id = cl.get(space, (char*)&(results[i].key), sizeof(uint64_t),
                &(results[i].get_status), &(results[i].attr), &(results[i].attr_size));
            num_loops++;
            if (results[i].get_status == HYPERDEX_CLIENT_INTERRUPTED) {
                WDEBUG << "get interrupted\n";
            }
        }
        if (results[i].op_id < 0) {
            WDEBUG << "\"get\" returned " << results[i].op_id << " with status " << results[i].get_status << std::endl;
        }
    }

    hyperdex_client_returncode loop_status = HYPERDEX_CLIENT_INTERRUPTED;
    int64_t loop_id = -1;
    // call loop once for every get
    //for (int i = 0; i < numNodes; i++) {
    for (int i = 0; i < num_loops; i++) {
        while (loop_id < 0 && loop_status == HYPERDEX_CLIENT_INTERRUPTED) {
            loop_id = cl.loop(-1, &loop_status);
            if (loop_status == HYPERDEX_CLIENT_INTERRUPTED) {
                WDEBUG << "loop interrupted\n";
            }
        }
        if (loop_id < 0) {
            WDEBUG << "get \"loop\" returned " << loop_id << " with status " << loop_status << std::endl;
            for (i = i-1; i >= 0; i--) {
                hyperdex_client_destroy_attrs(results[i].attr, results[i].attr_size);
            }
            std::vector<std::pair<uint64_t, uint64_t>> empty(0);
            return empty;
        }

        // double check this ID exists
        bool found = false;
        for (int i = 0; i < numNodes; i++) {
            found = found || (results[i].op_id == loop_id);
        }
        assert(found);
    }

    std::vector<std::pair<uint64_t, uint64_t>> toRet;
    for (int i = 0; i < numNodes; i++) {
        if (results[i].attr_size == 0) {
            WDEBUG << "Key " << results[i].key << " did not exist in hyperdex" << std::endl;
        } else if (results[i].attr_size > 1) {
            WDEBUG << "\"get\" number " << i << " returned " << results[i].attr_size << " attrs" << std::endl;
        } else {
            uint64_t *shard = (uint64_t*)results[i].attr->value;
            uint64_t nodeID = results[i].key;
            toRet.emplace_back(nodeID, *shard);
        }
        hyperdex_client_destroy_attrs(results[i].attr, results[i].attr_size);
    }
    WDEBUG << "get asked for " << numNodes << " mappings, returning " << toRet.size() << std::endl;
    return toRet;
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


