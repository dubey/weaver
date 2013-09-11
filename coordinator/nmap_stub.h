/*
 * ===============================================================
 *    Description:  Wrapper around HyperDex client for getting and
 *                  putting coordinator state-related mappings.
 *
 *        Created:  09/05/2013 11:18:57 AM
 *
 *         Author:  Greg Hill, gdh39@cornell.edu
 *                  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <vector>
#include <hyperdex/client.hpp>
#include <hyperdex/datastructures.h>
#include <po6/threads/mutex.h>

#include "common/weaver_constants.h"

namespace coordinator
{
    class nmap_stub
    {
        public:
            nmap_stub();

        private:
            const char *space = "weaver_node_mapping";
            const char *attrName = "shard"; // we get an attribute named "shard" with integer value correspoding to which shard node is placed on
            const char *host = HYPERDEX_COORD_IPADDR;
            static const int port = HYPERDEX_COORD_PORT;
            hyperdex::Client cl;
            po6::threads::mutex hyperclientLock;

        public:
            void put_mappings(std::vector<std::pair<uint64_t, uint64_t>> &pairs_to_add);
            void get_mappings(std::unordered_set<uint64_t> &toGet, std::unordered_map<uint64_t, uint64_t> &toPut);
            void clean_up_space();
    };

    inline
    nmap_stub :: nmap_stub() : cl(host, port) { }

    inline void
    nmap_stub :: put_mappings(std::vector<std::pair<uint64_t, uint64_t>> &pairs_to_add)
    {
        int numPairs = pairs_to_add.size();
        hyperdex_ds_arena *arena = hyperdex_ds_arena_create();
        hyperdex_client_attribute *attrs_to_add = hyperdex_ds_allocate_attribute(arena, numPairs);
        //hyperclient_attribute* attrs_to_add = (hyperclient_attribute *)malloc(numPairs * sizeof(hyperclient_attribute));

        hyperclientLock.lock();
        for (int i = 0; i < numPairs; i++) {
            attrs_to_add[i].attr = attrName;
            attrs_to_add[i].value = (char *) &pairs_to_add[i].second;
            attrs_to_add[i].value_sz = sizeof(int64_t);
            attrs_to_add[i].datatype = HYPERDATATYPE_INT64;

            hyperdex_client_returncode put_status;
            int64_t op_id = cl.put(space, (const char *) &pairs_to_add[i].first, sizeof(int64_t), &(attrs_to_add[i]), 1, &put_status);
            if (op_id < 0) {
                std::cerr << "\"put\" returned " << op_id << " with status " << put_status << std::endl;
                hyperclientLock.unlock();
                free(attrs_to_add);
                return;
            }
        }

        hyperdex_client_returncode loop_status;
        int64_t loop_id;
        // call loop once for every put
        for (int i = 0; i < numPairs; i++) {
            loop_id = cl.loop(-1, &loop_status);
            if (loop_id < 0) {
                std::cerr << "put \"loop\" returned " << loop_id << " with status " << loop_status << std::endl;
                hyperclientLock.unlock();
                free(attrs_to_add);
                return;
            }
        }
        hyperclientLock.unlock();
        //free(attrs_to_add);
        hyperdex_ds_arena_destroy(arena);
    }

    void
    nmap_stub :: get_mappings(std::unordered_set<uint64_t> &toGet, std::unordered_map<uint64_t, uint64_t> &toPut)
    {
        int numNodes = toGet.size();
        struct async_get {
            uint64_t key;
            int64_t op_id;
            hyperdex_client_returncode get_status;
            const hyperdex_client_attribute * attr;
            size_t attr_size;
        };
        std::vector<struct async_get > results(numNodes);
        auto nextHandle = toGet.begin();
        for (int i = 0; i < numNodes; i++) {
            results[i].key = *nextHandle;
            nextHandle++;
        }

        hyperclientLock.lock();
        for (int i = 0; i < numNodes; i++) {
            results[i].op_id = cl.get(space, (char *) &(results[i].key), sizeof(uint64_t), 
                &(results[i].get_status), &(results[i].attr), &(results[i].attr_size));
            nextHandle++;
            /*
            if (op_id < 0)
            {
                std::cerr << "\"get\" returned " << op_id << " with status " << get_status << std::endl;
                return std::vector<int64_t>(); // need to destroy_attrs
            }
            */
        }

        hyperdex_client_returncode loop_status;
        int64_t loop_id;
        // call loop once for every get
        for (int i = 0; i < numNodes; i++) {
            loop_id = cl.loop(-1, &loop_status);
            if (loop_id < 0) {
                std::cerr << "get \"loop\" returned " << loop_id << " with status " << loop_status << std::endl;
                return; // free previously gotten attrs
            }

            // double check this ID exists
            bool found = false;
            for (int i = 0; i < numNodes; i++) {
                found = found || (results[i].op_id == loop_id);
            }
            assert(found);
        }
        hyperclientLock.unlock();

        for (int i = 0; i < numNodes; i++) {
            if (results[i].attr_size != 1) {
                std::cerr << "\"get\" number " << i << " returned " << results[i].attr_size << " attrs" << std::endl;
            }
            uint64_t* shard = (uint64_t *) results[i].attr->value;
            uint64_t nodeID = results[i].key;
            assert(toPut.find(nodeID) == toPut.end());
            toPut.emplace(nodeID, *shard);
            //hyperclient_destroy_attrs(results[i].attr, results[i].attr_size);
        }
    }

    inline void
    nmap_stub :: clean_up_space()
    {
        //cl.rm_space(space);
    }

}
