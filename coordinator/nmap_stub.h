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
#include <hyperclient.h>
#include <hyperdex.h>

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
            hyperclient cl;

        public:
            // TODO are these thread-safe?
            void put_mappings(std::vector<std::pair<uint64_t, int64_t>> pairs_to_add);
            std::vector<int64_t> get_mappings(std::vector<uint64_t> handles);
            void clean_up_space();
    };

    inline
    nmap_stub :: nmap_stub() : cl(host, port) { }

    inline void
    nmap_stub :: put_mappings(std::vector<std::pair<uint64_t, int64_t>> pairs_to_add)
    {
        int numPairs = pairs_to_add.size();
        hyperclient_attribute* attrs_to_add = (hyperclient_attribute *)malloc(numPairs * sizeof(hyperclient_attribute));

        for (int i = 0; i < numPairs; i++) {
            attrs_to_add[i].attr = attrName;
            attrs_to_add[i].value = (char *) &pairs_to_add[i].second;
            attrs_to_add[i].value_sz = sizeof(int64_t);
            attrs_to_add[i].datatype = HYPERDATATYPE_INT64;

            hyperclient_returncode put_status;
            int64_t op_id = cl.put(space, (const char *) &pairs_to_add[i].first, sizeof(int64_t), &(attrs_to_add[i]), 1, &put_status);
            if (op_id < 0) {
                std::cerr << "\"put\" returned " << op_id << " with status " << put_status << std::endl;
            }
        }

        hyperclient_returncode loop_status;
        int64_t loop_id;
        // call loop once for every put
        for (int i = 0; i < numPairs; i++) {
            loop_id = cl.loop(-1, &loop_status);
            if (loop_id < 0) {
                std::cerr << "\"loop\" returned " << loop_id << " with status " << loop_status << std::endl;
                free(attrs_to_add);
                return;
            }
        }
        free(attrs_to_add);
    }

    inline std::vector<int64_t>
    nmap_stub :: get_mappings(std::vector<uint64_t> handles)
    {
        uint64_t numNodes = handles.size();
        hyperclient_returncode get_status;
        std::vector<std::pair<hyperclient_attribute *, size_t> > results(numNodes);

        for (uint64_t i = 0; i < numNodes; i++) {
            int64_t op_id = cl.get(space, (char *) &handles[i], sizeof(uint64_t), &get_status, &(results[i].first), &results[i].second);
            if (op_id < 0)
            {
                std::cerr << "\"get\" returned " << op_id << " with status " << get_status << std::endl;
                return std::vector<int64_t>(); // need to destroy_attrs
            }
        }

        hyperclient_returncode loop_status;
        int64_t loop_id;
        // call loop once for every get
        for (uint64_t i = 0; i < numNodes; i++) {
            loop_id = cl.loop(-1, &loop_status);
            if (loop_id < 0) {
                std::cerr << "\"loop\" returned " << loop_id << " with status " << loop_status << std::endl;
                return std::vector<int64_t>(); // need to destroy_attrs
            }
        }

        std::vector<int64_t> toRet(handles.size());
        for (uint64_t i = 0; i < numNodes; i++) {
            if (results[i].second != 1) {
                std::cerr << "\"get\" number " << i << " returned " << results[i].second << " attrs" << std::endl;
                return std::vector<int64_t>(); // need to destroy_attrs
            }
            int64_t* raw = (int64_t *) results[i].first->value;
            toRet[i] = *raw;
            hyperclient_destroy_attrs(results[i].first, results[i].second);
        }
        return toRet;
    }

    inline void
    nmap_stub :: clean_up_space()
    {
        cl.rm_space(space);
    }

}
