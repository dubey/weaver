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

#include <unordered_set>
#include <unordered_map>

// hash functions
namespace std
{
    template <typename T1, typename T2, typename T3>
    struct hash<tuple<T1, T2, T3>>
    {
        size_t operator()(const tuple<T1, T2, T3>& k) const
        {
            size_t val = hash<T1>()(get<0>(k));
            val ^= hash<T2>()(get<1>(k)) + 0x9e3779b9 + (val<<6) + (val>>2);
            val ^= hash<T3>()(get<2>(k)) + 0x9e3779b9 + (val<<6) + (val>>2);
            return val;
        }
    };

    template <typename T1, typename T2>
    struct hash<pair<T1, T2>>
    {
        size_t operator()(const pair<T1, T2>& k) const
        {
            size_t val = hash<T1>()(k.first);
            val ^= hash<T2>()(k.second) + 0x9e3779b9 + (val<<6) + (val>>2);
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

namespace weaver_util
{
    template<typename K, typename V>
    inline bool
    exists(const std::unordered_map<K, V> &map, const K &key)
    {
        return map.find(key) != map.end();
    }

    template<typename T>
    inline bool
    exists(const std::unordered_set<T> &set, const T &t)
    {
        return set.find(t) != set.end();
    }

    struct equint64_t
    {
        bool operator () (uint64_t u1, uint64_t u2) const
        {
            return u1 == u2;
        }
    };

    struct eqstr
    {
        bool operator() (const std::string &s1, const std::string &s2) const
        {
            return s1 == s2;
        }
    };
}


#endif
