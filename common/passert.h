/*
 * ===============================================================
 *    Description:  Assert, but print to log if false
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2014, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_common_passert_h_
#define weaver_common_passert_h_

#include <cassert>
#include <glog/logging.h>

#ifndef PASSERT
#define PASSERT(X) \
    if (!(X)) { \
        LOG(ERROR) << __FILE__ << ":" << __LINE__ << " assert fail: (" << #X << ")" << std::endl; \
    } \
    assert(X);
#endif

#endif
