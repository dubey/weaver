/*
 * ===============================================================
 *    Description:  Constant values used across the project
 *
 *        Created:  01/15/2013 03:01:04 PM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __CONSTANTS__
#define __CONSTANTS__

#include "stdint.h"

#define MAX_TIME UINT64_MAX
#define COORD_IPADDR "127.0.0.1"
#define COORD_PORT 5200
#define SHARD_IPADDR "127.0.0.1"
#define COORD_REC_PORT 4200
#define NUM_SHARDS 1
#define MAX_PORT (COORD_PORT + NUM_SHARDS)
#define SEND_PORT_INCR 1000 // outgoing port increment for shard servers

#endif
