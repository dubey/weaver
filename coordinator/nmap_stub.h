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
            static const char *space = "weaver_node_mapping";
            static const char *attrName = "shard"; // we get an attribute named "shard" with integer value correspoding to which shard node is placed on
            static const char *host = HYPERDEX_COORD_IPADDR;
            static int port = HYPERDEX_COORD_PORT;
            hyperclient cl;

        public:
            void put_mappings(std::vector<std::pair<uint64_t, int64_t>> pairs_to_add);
            std::vector<int> get_mappings(std::vector<uint64_t> handles);
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

    inline void
    nmap_stub :: put_mappings(std::vector<std::pair<uint64_t, int64_t> > pairs_to_add)
    {
        int numPairs = pairs_to_add.size();
        hyperclient_attribute* attrs_to_add = (hyperclient_attribute *) malloc(numPairs * sizeof(hyperclient_attribute));

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
}
