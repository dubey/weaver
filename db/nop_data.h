/*
 * ===============================================================
 *    Description:  Data associated with periodic heartbeat
 *                  message sent from each vector timestamper to
 *                  each shard.       
 *
 *        Created:  09/19/2013 09:43:24 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "node_prog/node_prog_type.h"

struct nop_data
{
    uint64_t vt_id;
    uint64_t req_id;
    std::vector<std::pair<uint64_t, node_prog::prog_type>> done_reqs;
};
