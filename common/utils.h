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

#include <vector>
#include <tuple>
#include <unordered_set>
#include <unordered_map>
#include <po6/io/fd.h>
#include <fstream>
#include <openssl/sha.h>

#include "common/MurmurHash3.h"

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

    template<typename T>
    struct murmur_hasher
    {
        size_t operator()(const T &t) const
        {
            uint64_t hash[2];
            MurmurHash3_x64_128(&t, sizeof(t), 0, hash);
            return hash[0];
        }
    };

    template<>
    struct murmur_hasher<std::string>
    {
        size_t operator()(const std::string &t) const
        {
            uint64_t hash[2];
            MurmurHash3_x64_128(t.c_str(), t.size(), 0, hash);
            return hash[0];
        }
    };

    inline uint64_t
    urandom_uint64()
    {
        uint64_t token;
        po6::io::fd sysrand(open("/dev/urandom", O_RDONLY));

        if (sysrand.get() < 0
         || sysrand.read(&token, sizeof(token)) != sizeof(token)) {
            token = 0;
        }

        return token;
    }

    inline std::string
    sha256_char_to_string(const unsigned char *hash)
    {
        char out_buf[65];
        for(size_t i = 0; i < SHA256_DIGEST_LENGTH; i++) {
            sprintf(out_buf + (i * 2), "%02x", hash[i]);
        }
        out_buf[64] = 0;

        return std::string(out_buf, 65);
    }

    inline std::string
    sha256_chararr(const char *data, size_t data_sz)
    {
        SHA256_CTX sha256;
        SHA256_Init(&sha256);
        SHA256_Update(&sha256, data, data_sz);
        unsigned char hash[SHA256_DIGEST_LENGTH];
        SHA256_Final(hash, &sha256);

        return sha256_char_to_string(hash);
    }

    inline std::string
    sha256_file(const std::string &file)
    {
        std::ifstream read_f;
        read_f.open(file, std::ifstream::in);
        if (!read_f) {
            return "";
        }

        read_f.seekg(0, read_f.end);
        size_t file_sz = read_f.tellg();
        read_f.seekg(0, read_f.beg);

        SHA256_CTX sha256;
        SHA256_Init(&sha256);

        size_t buf_sz = 1024;
        char buf[1024];
        for (size_t rem = file_sz; rem > 0;) {
            size_t bytes_read = (rem > buf_sz? buf_sz : rem);
            read_f.read(buf, bytes_read);
            rem -= bytes_read;
            SHA256_Update(&sha256, buf, bytes_read);
        }
        read_f.close();

        unsigned char file_hash[SHA256_DIGEST_LENGTH];
        SHA256_Final(file_hash, &sha256);

        return sha256_char_to_string(file_hash);
    }
}


#endif
