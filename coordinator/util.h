/*
 * ===============================================================
 *    Description:  Server manager util methods.
 *
 *        Created:  2014-02-09 17:06:10
 *
 *         Author:  Robert Escriva, escriva@cs.cornell.edu
 *                  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

// Most of the following code has been 'borrowed' from
// Robert Escriva's HyperDex coordinator.
// see https://github.com/rescrv/HyperDex for the original code.

#include "common/server_manager_returncode.h"

static inline void
generate_response(rsm_context* ctx, server_manager_returncode x)
{
    const char* ptr = nullptr;

    switch (x)
    {
        case COORD_SUCCESS:
            ptr = "\x22\x80";
            break;
        case COORD_MALFORMED:
            ptr = "\x22\x81";
            break;
        case COORD_DUPLICATE:
            ptr = "\x22\x82";
            break;
        case COORD_NOT_FOUND:
            ptr = "\x22\x83";
            break;
        case COORD_UNINITIALIZED:
            ptr = "\x22\x85";
            break;
        case COORD_NO_CAN_DO:
            ptr = "\x22\x87";
            break;
        default:
            ptr = "\xff\xff";
            break;
    }

    rsm_set_output(ctx, ptr, 2);
}

#define INVARIANT_BROKEN(X) \
    fprintf(log, "invariant broken at " __FILE__ ":%d:  %s\n", __LINE__, X "\n")
