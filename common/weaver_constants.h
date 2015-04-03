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

#include <stdint.h>
#include <iostream>
#include <glog/logging.h>

// debugging
#ifndef WDEBUG
#ifdef weaver_debug_
#define WDEBUG LOG(INFO)
#else
#define WDEBUG if (false) std::cerr << __FILE__ << ":" << __LINE__ << " "
#endif
#endif

#ifndef weaver_common_constants_h_
#define weaver_common_constants_h_

// if benchmarking
//#define weaver_benchmark_

// unused expression for no warnings
#define UNUSED(exp) do { (void)(exp); } while (0)

#define NANO (1000000000ULL)
#define GIGA (1000000000ULL)
#define MEGA (1000000UL)

#endif
