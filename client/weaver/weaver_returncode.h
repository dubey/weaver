/*
 * ===============================================================
 *    Description:  Weaver client returncodes.
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2015, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_client_weaver_returncode_h_
#define weaver_client_weaver_returncode_h_

#ifdef __cplusplus
extern "C"
{
#endif

enum weaver_client_returncode
{
    WEAVER_CLIENT_SUCCESS,
    WEAVER_CLIENT_INITERROR,
    WEAVER_CLIENT_LOGICALERROR,
    WEAVER_CLIENT_NOMEM,
    WEAVER_CLIENT_EXCEPTION,
    // tx
    WEAVER_CLIENT_ABORT,
    WEAVER_CLIENT_ACTIVETX,
    WEAVER_CLIENT_NOACTIVETX,
    WEAVER_CLIENT_NOAUXINDEX,
    // node prog
    WEAVER_CLIENT_NOTFOUND,
    WEAVER_CLIENT_BADPROGTYPE,
    WEAVER_CLIENT_DLOPENERROR,
    // msg
    WEAVER_CLIENT_DISRUPTED,
    WEAVER_CLIENT_INTERNALMSGERROR,
    // benchmarks
    WEAVER_CLIENT_BENCHMARK
};

const char*
weaver_client_returncode_to_string(enum weaver_client_returncode code);

#ifdef __cplusplus
}
#endif

#endif
