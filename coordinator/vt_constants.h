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

// number of cores in uint64
#define NUM_PROCS_ONLINE ((uint64_t)sysconf(_SC_NPROCESSORS_ONLN))

// usually number of worker threads = (number of cores - 1) because we have 1 nop thread
// when only 1 core then number of worker threads = 1
#define NUM_VT_THREADS (NUM_PROCS_ONLINE>1? (NUM_PROCS_ONLINE-1) : 1)

#endif
