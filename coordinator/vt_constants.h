/*
 * ===============================================================
 *    Description:  Constants for timestamper.
 *
 *        Created:  09/21/2014 11:29:23 AM
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_coordinator_vt_constants_h_
#define weaver_coordinator_vt_constants_h_

#define VT_TIMEOUT_NANO 1000 // number of nanoseconds between successive nops
#define VT_CLK_TIMEOUT_NANO 1000000 // number of nanoseconds between vt gossip
#define NUM_VT_THREADS 128

#endif
