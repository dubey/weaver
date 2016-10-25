/*
 * ===============================================================
 *    Description:  Implement returncode_to_string
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2014, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include "client/weaver/weaver_returncode.h"

#ifdef __cplusplus
extern "C"
{
#endif

const char*
weaver_client_returncode_to_string(enum weaver_client_returncode code)
{
    switch (code) {
        case WEAVER_CLIENT_SUCCESS:
            return "WEAVER_CLIENT_SUCCESS";
        case WEAVER_CLIENT_INITERROR:
            return "WEAVER_CLIENT_INITERROR";
        case WEAVER_CLIENT_LOGICALERROR:
            return "WEAVER_CLIENT_LOGICALERROR";
        case WEAVER_CLIENT_NOMEM:
            return "WEAVER_CLIENT_NOMEM";
        case WEAVER_CLIENT_EXCEPTION:
            return "WEAVER_CLIENT_EXCEPTION";
        case WEAVER_CLIENT_TIMEOUT:
            return "WEAVER_CLIENT_TIMEOUT";
        case WEAVER_CLIENT_ABORT:
            return "WEAVER_CLIENT_ABORT";
        case WEAVER_CLIENT_ACTIVETX:
            return "WEAVER_CLIENT_ACTIVETX";
        case WEAVER_CLIENT_NOACTIVETX:
            return "WEAVER_CLIENT_NOACTIVETX";
        case WEAVER_CLIENT_NOAUXINDEX:
            return "WEAVER_CLIENT_NOAUXINDEX";
        case WEAVER_CLIENT_NOTFOUND:
            return "WEAVER_CLIENT_NOTFOUND";
        case WEAVER_CLIENT_BADPROGTYPE:
            return "WEAVER_CLIENT_BADPROGTYPE";
        case WEAVER_CLIENT_DLOPENERROR:
            return "WEAVER_CLIENT_DLOPENERROR";
        case WEAVER_CLIENT_DISRUPTED:
            return "WEAVER_CLIENT_DISRUPTED";
        case WEAVER_CLIENT_INTERNALMSGERROR:
            return "WEAVER_CLIENT_INTERNALMSGERROR";
        case WEAVER_CLIENT_BENCHMARK:
            return "WEAVER_CLIENT_BENCHMARK";
        default:
            return "unknown weaver returncode";
    }
}

#ifdef __cplusplus
}
#endif
