/*
 * ===============================================================
 *    Description:  Commonly used functionality.
 *
 *        Created:  2014-07-07 13:26:12
 *
 *         Author:  Ayush Dubey, dubey@cs.cornell.edu
 *
 * Copyright (C) 2013, Cornell University, see the LICENSE file
 *                     for licensing agreement
 * ===============================================================
 */

#ifndef weaver_common_utils_h_
#define weaver_common_utils_h_

// hash functions
namespace std
{
    template <typename T1, typename T2, typename T3>
    struct hash<tuple<T1, T2, T3>>
    {
        size_t operator()(const tuple<T1, T2, T3>& k) const
        {
            size_t val = hash<uint64_t>()(get<0>(k));
            val ^= hash<uint64_t>()(get<1>(k)) + 0x9e3779b9 + (val<<6) + (val>>2);
            val ^= hash<uint64_t>()(get<2>(k)) + 0x9e3779b9 + (val<<6) + (val>>2);
            return val;
        }
    };

    template <typename T1, typename T2>
    struct hash<pair<T1, T2>>
    {
        size_t operator()(const pair<T1, T2>& k) const
        {
            size_t val = hash<uint64_t>()(k.first);
            val ^= hash<uint64_t>()(k.second) + 0x9e3779b9 + (val<<6) + (val>>2);
            return val;
        }
    };

    template <>
    struct hash<vector<uint64_t>> 
    {
        public:
            size_t operator()(const vector<uint64_t> &v) const throw() 
            {
                if (v.empty()) {
                    return hash<uint64_t>()(0);
                }
                size_t val = hash<uint64_t>()(v[0]);
                for (size_t i = 1; i < v.size(); i++) {
                    val ^= hash<uint64_t>()(v[i]) + 0x9e3779b9 + (val<<6) + (val>>2);
                }
                return val;
            }
    };
}


#endif
