/*
 * ===============================================================
 *    Description:  Helper methods for std::vector<bool> with some
 *                  bitset functionality.
 *
 *        Created:  2014-06-25 12:02:54
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_common_bool_vector_h_
#define weaver_common_bool_vector_h_

#include <vector>

namespace weaver_util
{
    inline bool
    all(const std::vector<bool> &v)
    {
        for (size_t i = 0; i < v.size(); i++) {
            if (!v[i]) {
                return false;
            }
        }
        return true;
    }

    inline bool
    any(const std::vector<bool> &v)
    {
        for (size_t i = 0; i < v.size(); i++) {
            if (v[i]) {
                return true;
            }
        }
        return false;
    }

    inline bool
    none(const std::vector<bool> &v)
    {
        for (size_t i = 0; i < v.size(); i++) {
            if (v[i]) {
                return false;
            }
        }
        return true;
    }

    inline void
    set_bool_vec(std::vector<bool> &vec, bool val)
    {
        for (size_t i = 0; i < vec.size(); i++) {
            vec[i] = val;
        }
    }

    inline void
    set_all(std::vector<bool> &v)
    {
        set_bool_vec(v, true);
    }

    inline void
    reset_all(std::vector<bool> &v)
    {
        set_bool_vec(v, false);
    }
}

#endif
