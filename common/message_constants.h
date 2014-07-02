/*
 * ===============================================================
 *    Description:  Constant values related to messaging layer.
 *
 *        Created:  2014-06-19 12:56:05
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_common_message_constants_h_
#define weaver_common_message_constants_h_

#define ID_INCR (1ULL << 32ULL)
#define WEAVER_TO_BUSYBEE(x) (x+ID_INCR)
#define BUSYBEE_TO_WEAVER(x) (x-ID_INCR)
#define CLIENT_ID_INCR (CLIENT_ID + ID_INCR)

#endif
