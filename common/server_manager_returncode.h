/*
 * ===============================================================
 *    Description:  Return codes from server manager to shards
 *                  and timestampers.
 *
 *        Created:  2014-02-09 17:11:00
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

#ifndef weaver_common_server_manager_returncode_h_
#define weaver_common_server_manager_returncode_h_

#include <iostream>

#define XSTR(x) #x
#define STR(x) XSTR(x)
#define STRINGIFY(x) case (x): lhs << STR(x); break
#define CSTRINGIFY(x) case (x): return STR(x);

// occupies [8832, 8960)
// these are hardcoded as byte strings in coordinator/coordinator.cc
// keep them in sync
enum server_manager_returncode
{
    COORD_SUCCESS = 8832,
    COORD_MALFORMED = 8833,
    COORD_DUPLICATE = 8834,
    COORD_NOT_FOUND = 8835,
    COORD_UNINITIALIZED = 8837,
    COORD_NO_CAN_DO = 8839
};

//std::ostream&
//operator << (std::ostream& lhs, server_manager_returncode rhs)
//{
//    switch (rhs)
//    {
//        STRINGIFY(COORD_SUCCESS);
//        STRINGIFY(COORD_MALFORMED);
//        STRINGIFY(COORD_DUPLICATE);
//        STRINGIFY(COORD_NOT_FOUND);
//        STRINGIFY(COORD_UNINITIALIZED);
//        STRINGIFY(COORD_NO_CAN_DO);
//        default:
//            lhs << "unknown server_manager_returncode";
//            break;
//    }
//
//    return lhs;
//}

#endif
