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

#ifndef weaver_common_clock_h_
#define weaver_common_clock_h_

#include <time.h>

#include "common/weaver_constants.h"

namespace wclock
{
    class weaver_timer
    {
        public:
            timespec ts;
            void get_clock();
            weaver_timer() { get_clock(); }
            double get_secs() { return ts.tv_sec + ((double)ts.tv_nsec)/GIGA; }
            uint64_t get_nanosecs() { return ts.tv_nsec + ((uint64_t)ts.tv_sec)*GIGA; }
            // return nanosecs elapsed since some fixed time
            uint64_t get_time_elapsed();
            // get_time_elapsed / 1000000
            uint64_t get_time_elapsed_millis();
            uint64_t get_real_time();
            uint64_t get_real_time_millis();
            weaver_timer& operator-=(const weaver_timer &rhs);
    };

    inline weaver_timer
    operator-(weaver_timer lhs, const weaver_timer &rhs)
    {
        if ((lhs.ts.tv_nsec-rhs.ts.tv_nsec)<0) {
            lhs.ts.tv_sec  = lhs.ts.tv_sec - rhs.ts.tv_sec - 1;
            lhs.ts.tv_nsec = GIGA + lhs.ts.tv_nsec - rhs.ts.tv_nsec;
        } else {
            lhs.ts.tv_sec  -= rhs.ts.tv_sec;
            lhs.ts.tv_nsec -= rhs.ts.tv_nsec;
        }
        return lhs;
    }

    inline weaver_timer&
    weaver_timer :: operator-=(const weaver_timer &rhs)
    {
        *this = *this - rhs;
        return *this;
    }
}
#endif
