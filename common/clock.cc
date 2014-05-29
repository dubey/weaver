/*
 * ===============================================================
 *    Description:  Timing-related functions implementation.
 *
 *        Created:  2014-05-29 12:22:12
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#include <stdint.h>
#ifdef __MACH__
#include <mach/clock.h>
#include <mach/mach.h>
#endif

#include "common/clock.h"

using wclock::weaver_timer;

void
weaver_timer :: get_clock()
{
#ifdef __MACH__ // OS X does not have clock_gettime, use clock_get_time
    clock_serv_t cclock;
    mach_timespec_t mts;
    host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
    clock_get_time(cclock, &mts);
    mach_port_deallocate(mach_task_self(), cclock);
    ts.tv_sec = mts.tv_sec;
    ts.tv_nsec = mts.tv_nsec;
#else
    clock_gettime(CLOCK_MONOTONIC, &ts);
#endif
}

uint64_t
weaver_timer :: get_time_elapsed()
{
    uint64_t ret = 0;
    get_clock();
    ret += ts.tv_sec*GIGA + ts.tv_nsec;
    return ret;
}

uint64_t
weaver_timer :: get_time_elapsed_millis()
{
    uint64_t ret = get_time_elapsed() / MEGA;
    return ret;
}
