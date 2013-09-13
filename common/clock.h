/*
 * ===============================================================
 *    Description:  Get current clock value, for both Posix and
 *                  Mac
 *
 *        Created:  06/21/13 14:43:40
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef __WEAVER_GET_CLOCK__
#define __WEAVER_GET_CLOCK__

#include <time.h>
#include <sys/time.h>

#ifdef __MACH__
#include <mach/clock.h>
#include <mach/mach.h>
#endif

#include "weaver_constants.h"

#define GIGA (1000000000UL)
#define MEGA (1000000UL)

namespace wclock
{

    void get_clock(timespec *ts)
    {
#ifdef __MACH__ // OS X does not have clock_gettime, use clock_get_time
        clock_serv_t cclock;
        mach_timespec_t mts;
        host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
        clock_get_time(cclock, &mts);
        mach_port_deallocate(mach_task_self(), cclock);
        ts->tv_sec = mts.tv_sec;
        ts->tv_nsec = mts.tv_nsec;
#else
        clock_gettime(CLOCK_MONOTONIC, ts);
#endif
    }

    // return nanosecs elapsed since some fixed time
    uint64_t get_time_elapsed(timespec &ts)
    {
        uint64_t ret = 0;
        get_clock(&ts);
        ret += ts.tv_sec * GIGA + ts.tv_nsec;
        return ret;
    }

    // get_time_elapsed / 1000000
    uint64_t get_time_elapsed_millis(timespec &ts)
    {
        uint64_t ret = get_time_elapsed(ts) / MEGA;
        return ret;
    }

    double diff(timespec &start, timespec &end)
    {
        timespec temp;
        if ((end.tv_nsec-start.tv_nsec)<0) {
            temp.tv_sec = end.tv_sec-start.tv_sec-1;
            temp.tv_nsec = GIGA + end.tv_nsec-start.tv_nsec;
        } else {
            temp.tv_sec = end.tv_sec-start.tv_sec;
            temp.tv_nsec = end.tv_nsec-start.tv_nsec;
        }
        double ret = temp.tv_sec;
        ret += (((double)temp.tv_nsec) / GIGA);
        return ret;
    }

}
#endif
